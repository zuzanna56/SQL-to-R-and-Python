# Rozwiazania zadan pracy domowej numer 2
# 12-05-2022
# Zuzanna Pirog IAD J6




#musimy sie upewnic ze kolumny napisow nie zostana zmienione na typ factor
options(stringsAsFactors=FALSE)

library("sqldf")
library("data.table")
library("dplyr")
library("compare")
library("stringi")

#wczytujemy ramki danych
Badges <- read.csv("Badges.csv.gz")
Comments <- read.csv("Comments.csv.gz")
PostLinks <- read.csv("PostLinks.csv.gz")
Posts <- read.csv("Posts.csv.gz")
Tags <- read.csv("Tags.csv.gz")
Users <- read.csv("Users.csv.gz")
Votes <- read.csv("Votes.csv.gz")

#Rozwiazanie przy pomocy pakietu sqldf

# Z ramki danych Tags wybieramy dwie kolumny - "Counts" i "Tags"
# Nastêpnie wybieramy tylko te wiersze, w ktorych parametr wartosc w kolumnie"Counts"
# jest wieksza niz 1000 nastepnie poradkujemy wiersze w kolejnosci Descending
# po kolumnie Count
df_sql_1 <- function(Tags){
  return(sqldf("SELECT Count, TagName
               FROM Tags
               WHERE Count > 1000
               ORDER BY Count DESC"))
}
df_sql_1(Tags)




df_sql_2 <- function(Users, Posts){
  return(sqldf("SELECT Location, COUNT(*) AS Count
                FROM (
                SELECT Posts.OwnerUserId, Users.Id, Users.Location
                FROM Users
                JOIN Posts ON Users.Id = Posts.OwnerUserId
                )
                WHERE Location NOT IN ('')
                GROUP BY Location
                ORDER BY Count DESC
                LIMIT 10"))
}

df_sql_2(Users, Posts)


# Z ramki Badges wybieramy wiersze w ktorych w kolumnie "Class" wartosc wynosi 1,
# a nastepnie kolumny "Name" oraz "Date", gdzie tej drugiej
# zmieniamy nazwe na Year i formatujemy by pokazywala wylacznie rok
# grupujemy po kolumnach Name i Year i liczymy liczbe wierszy zapisujac je
# w kolumnie Number. Nastepnie sumujemy po kolumnie Number grupujac po kolumnie Year
# i zapisujemy wyniki w kolumnie "TotalNumber"
# ustalamy kolejnosc po ascending TotalNumber
df_sql_3 <- function(Badges){
  return(sqldf("SELECT Year, SUM(Number) AS TotalNumber
                FROM (
                SELECT
                Name,
                COUNT(*) AS Number,
                STRFTIME('%Y', Badges.Date) AS Year
                FROM Badges
                WHERE Class = 1
                GROUP BY Name, Year
                )
                GROUP BY Year
                ORDER BY TotalNumber"))
}
df_sql_3(Badges)

df_sql_4 <- function(Users, Posts){
  return(sqldf("SELECT
                Users.AccountId,
                Users.DisplayName,
                Users.Location,
                AVG(PostAuth.AnswersCount) as AverageAnswersCount
                FROM
                (
                SELECT
                AnsCount.AnswersCount,
                Posts.Id,
                Posts.OwnerUserId
                FROM (
                SELECT Posts.ParentId, COUNT(*) AS AnswersCount
                FROM Posts
                WHERE Posts.PostTypeId = 2
                GROUP BY Posts.ParentId
                ) AS AnsCount
                JOIN Posts ON Posts.Id = AnsCount.ParentId
                ) AS PostAuth
                JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
                GROUP BY OwnerUserId
                ORDER BY AverageAnswersCount DESC, AccountId ASC
                LIMIT 10"))
}

df_sql_4(Users, Posts)

df_sql_5 <- function(Posts, Votes){
  return(sqldf("SELECT Posts.Title, Posts.Id,
                STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date,
                VotesByAge.Votes
                FROM Posts
                JOIN (
                SELECT
                PostId,
                MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
                MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
                SUM(Total) AS Votes
                FROM (
                SELECT
                PostId,
                CASE STRFTIME('%Y', CreationDate)
                WHEN '2021' THEN 'new'
                WHEN '2020' THEN 'new'
                ELSE 'old'
                END VoteDate,
                COUNT(*) AS Total
                FROM Votes
                WHERE VoteTypeId IN (1, 2, 5)
                GROUP BY PostId, VoteDate
                ) AS VotesDates
                GROUP BY VotesDates.PostId
                HAVING NewVotes > OldVotes
                ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
                WHERE Title NOT IN ('')
                ORDER BY Votes DESC
                LIMIT 10"))
}


df_sql_5(Posts, Votes)

#--------------------------------------------------
#rozwiazanie przy pomocy funkcji bazowych


#pierwsze bazowe

df_base_1 <- function(Tags){
  #ograniczenie ramki danych do dwoch kolumn
  df <- Tags[c("Count", "TagName")]
  #wybieramy tylko te ktorych count > 1000
  df <- df[Tags$Count > 1000, drop = FALSE,]
  #ustawiamy kolejnosc
  df <- df[order(df$Count, decreasing = TRUE), ]
  #przywracamy numery wierszy
  rownames(df) <- NULL
  
  return(df)
  
}



all.equal(df_base_1(Tags), df_sql_1(Tags))

#drugie bazowe

df_base_2 <- function(Users, Posts){
  #wybieramy potrzebne kolumny
  df_posts <- Posts[c("OwnerUserId")]
  df_users <- Users[c("Id", "Location")]
  #laczymy kolumny
  ans <- merge(df_posts, df_users, by.x = "OwnerUserId", by.y ="Id" )
  #tworzymy warunek
  '%notin%' <- Negate('%in%')
  ans <- ans[ans$Location %notin% c(''),]
  #liczymy liczbe lokalizacji
  ans <- aggregate(x = ans, by = ans[c("Location")], FUN = length)
  #zmieniamy nazwe 3. kolumny
  colnames(ans)[3] <- "Count"
  #usuwamy niepotrzebna kolumne
  ans <- ans[-2]
  #ustawiamy kolejnosc
  ans <- ans[order(ans$Count, decreasing = TRUE),]
  #zmieniamy na domyslne ustawienie wierszy
  rownames(ans) <- NULL
  return (head(ans, 10))
}

all.equal(df_sql_2(Users, Posts), df_base_2(Users, Posts))


#trzecie bazowe


df_base_3 <- function(Badges) {
  #stowrzmy pomocnicza ramke, nie jest to krok konieczny ale zostal tutaj zastosowany
  df_badges <- Badges
  #najpierw sformatujmy kolumne Badges$Date tak aby zawierala sam rok
  df_badges$Date <- strftime(df_badges$Date, format="%Y")
  #wybierzmy te wiersze w ktorych Class = 1
  df_badges <- df_badges[df_badges$Class==1,]
  #policzmy ile mamy rekordow
  ans <- aggregate(df_badges$Name, by= df_badges["Date"], FUN=length)
  #zmienmy nazwe kolumn
  colnames(ans) <- c("Year", "TotalNumber")
  #ustalamy odpowiednia kolejnpsc wierszy
  ans <- ans[order(ans$TotalNumber, decreasing = FALSE),]
  #zmieniamy nazwy wierszy na kolejne liczby calkowite
  rownames(ans) <- NULL
  ans
}

all.equal(df_sql_3(Badges), df_base_3(Badges))


#czwarte bazowe


df_base_4 <- function(Users, Posts){
  #wejdzmy do "srodka funkcji" i zajmijmy sie najpierw AnsCount
  #wybieramy interesujace nas kolumny, czyli takie gdzie PostTypeId = 2
  AnsCount <- Posts[Posts$PostTypeId == 2, c("ParentId", "PostTypeId"), drop = FALSE]
  #Policzmy ile mamy wynikow
  AnsCount <- aggregate(x = AnsCount, by = AnsCount["ParentId"], FUN = length)
  #zmienmy nazwe kolumny
  colnames(AnsCount)[2] <- "AnswersCount"
  #odrzucmy niepotrzebna kolumne
  AnsCount <- AnsCount[1:2]
  
  #przejdzmy teraz oczko wyzej i wybierzmy ponownie interesujace nas kolumny z ramki Posts
  df_posts <- Posts[c("Id", "OwnerUserId")]
  #zlaczmy otrzymana ramke i AnsCount
  PostAuth <- merge(df_posts, AnsCount, by.x = "Id", by.y = "ParentId")
  
  #przejdzmy do ostatniego oczka i ponownie wybierzmy interesujace nas kolumny
  #tym razem z ramki Users
  df_users <- Users[c("AccountId", "DisplayName", "Location")]
  #obliczmy srednia dla AnswersCount
  aver_ansCount <- aggregate(x = PostAuth["AnswersCount"], by = PostAuth["OwnerUserId"], FUN = mean)
  #zlaczmy obliczona ramke i ramke df_users
  ans <- merge(df_users, aver_ansCount, by.x = "AccountId", by.y = "OwnerUserId")
  #zmienmy nazwe kolumny na wymagana w zadaniu
  colnames(ans)[4] <-"AverageAnswersCount"
  #znajdzmy odpoweidnia permutacje podana w zadaniu i ja zastosujmy
  permutacja <- order(ans$AverageAnswersCount, ans$AccountId, decreasing = c(T, F))
  ans <- ans[permutacja,]
  #przywrocmy bazowe nazwy wierszy
  rownames(ans) <- NULL
  #zwracamy z limitem 10
  return(head(ans, 10))
}

all.equal(df_base_4(Users, Posts),df_sql_4(Users, Posts) )


#piate bazowe

df_base_5 <- function(Posts, Votes){
  #tworazymy ramke pomocnicza
  Votes_d <- Votes
  #formatujemy dane w kolumnie CreationDate
  Votes_d$CreationDate <- strftime(Votes_d$CreationDate, format = "%Y")
  #zmieniamy zawartosc powyzszej kolumny rozwazajac odpowiednie przypadki
  Votes_d$CreationDate <- sapply(Votes_d$CreationDate, switch,
                                 '2021' = 'new',
                                 '2020' = 'new', 'old')
  #filtrujemy wiersze ramki by spelnialy warunek
  Votes_d <- Votes_d[Votes_d$VoteTypeId %in% c(1, 2, 5), ]
  #zmieniamy nazwe kolumny na wymagana z polecenia
  colnames(Votes_d)[4] <- "VoteDate"
  #liczymy liczbe rekordow odpowiednio grupujac cala ramke
  VotesDates <- aggregate(Votes_d$PostId, by = Votes_d[c('PostId', 'VoteDate')], FUN = length)
  #zmieniamy nazwe kolumny na wymagana z polecenia
  colnames(VotesDates)[3] <- "Total"
  
  #tworzymy nowe kolumny, ktore rozwazaja dwa przypadki
  VotesDates$NewVotes <- ifelse(VotesDates$VoteDate == "new", VotesDates$Total, 0)
  VotesDates$OldVotes <- ifelse(VotesDates$VoteDate == "old", VotesDates$Total, 0)
  #tworzymy nowa ramke i wybieramy odpowiednie kolumny z ramki VotesDates
  VotesByAge <- VotesDates[, c("PostId", "Total", "NewVotes", "OldVotes")]
  
  #tworzymy 3 nowe ramki pomocnicze ktore zsumuja odpowiednio wiersze po grupowaniu wzgledem PostId
  df_total <- as.data.frame(aggregate(VotesByAge$Total, by = VotesByAge["PostId"], sum))
  colnames(df_total)[2] <- "Votes"
  df_new <- as.data.frame(aggregate(VotesByAge$NewVotes, by = VotesByAge["PostId"], sum))
  colnames(df_new)[2] <- "NewVotes"
  df_old <- as.data.frame(aggregate(VotesByAge$OldVotes, by = VotesByAge["PostId"], sum))
  colnames(df_old)[2] <- "OldVotes"
  
  #laczymy powyzsze 3 ramki wzgledem kolumny PostId
  df_temp <- merge(df_new, df_old, by = "PostId")
  VotesByAge <- merge(df_temp, df_total, by = "PostId")
  
  #filtrujemy odpowiednio wierwsze
  VotesByAge <- VotesByAge[VotesByAge$NewVotes > VotesByAge$OldVotes, ]
  
  #tworzymy pomocnicza ramke z ramki Posts z wybranymi interesujacymi nas kolumnami
  df_posts <- Posts[c("Title", "Id", "CreationDate")]
  #tworzymy nowa kolumne ze sformatowana kolumna CreationDate
  df_posts$Date <- stri_sub(df_posts[,"CreationDate"], 1L, 10L)
  #filtrujemy odpowiednio ramke by w kolumnie Title nie bylo pustych rekordoq
  df_posts <- df_posts[df_posts$Title != '', ]
  #wynikowa ramke otrzymujemy przez zlaczenie obu ramek wzgledem kolumn Id i PostId
  ans <- merge(df_posts, VotesByAge, by.x = "Id", by.y = "PostId")
  #wybieramy odpowiednie kolumny
  ans <- ans[,c("Title", "Id", "Date", "Votes")]
  #ustalamy kolejnosc
  ans  <- ans[order(ans$Votes, decreasing = TRUE), ]
  #zmieniamy typ danych w kolumnie Votes
  ans$Votes <- as.integer(ans$Votes)
  #ustalamy nowe numerowanie wierszy
  row.names(ans) <- NULL
  #zwracamy 10 rekordow
  ans <- ans[1:10,]
}

all.equal(df_base_5(Posts, Votes), df_sql_5(Posts, Votes))


#---------------------------------------------------
#rozwiazania przy pomocy pakietu dplyr
suppressMessages(library(dplyr))


#pierwsze dplyr

df_dplyr_1 <-function(Tags){
  dp <- select(Tags, Count, TagName)%>% #wybieramy interesujace nas kolumny
    filter(Count > 1000)%>% #filtrujemy wiersze ktore nas interesuja
    arrange(desc(Count)) #zmieniamy kolejnosc na descending po Count
  #zwracamy pomocnicza ramke
  dp
  
}
all.equal(df_dplyr_1(Tags), df_sql_1(Tags))


#drugie dplyr
df_dplyr_2 <- function(Users, Posts){
  dp <- Users%>% #najpierw wybieramy ramke users
    select(Id, Location)%>% #wybieramy interesujace nas kolumny z niej
    filter(Location != '')%>% #wybieramy interesujace nas wiersze
    inner_join(Posts, by = c('Id' = 'OwnerUserId'))%>% #laczymy ze soba dwie ramki 
    group_by(Location)%>% #grupujemy po Lokalizacji
    summarise(Count = n())%>% #podliczamy liczbe wierszy
    arrange(desc(Count))%>% #zmieniamy kolejnosc na descending po Count
    head(10)%>%
    as.data.frame()
}


all.equal(df_sql_2(Users, Posts), df_dplyr_2(Users, Posts))



#trzecie dplyr 
df_dplyr_3 <- function(Badges){
  dp <- Badges %>% 
    filter(Class == 1)%>% #filtrujemy odpoweidnie wiersze
    select(Name, Date)%>% #wybieramy kolumny
    mutate(Year = substr(Date, 1, 4))%>% #tworzymy nowa kolumne zawierajaca sfromatowana kolumne Date
    select(Name, Year)%>% #wybieramy dwie kolumny
    group_by(Name, Year)%>% #grupujemy bo dwoch kolumnach
    summarise(Number = n(), .groups = 'keep')%>% #liczymy liczbe rekordow
    group_by(Year)%>% #ponownie grupujemy
    summarise(TotalNumber = sum(Number), .groups = 'keep')%>% #tworzymy nowo kolumne z sumowanymi wynikami z kol. Number
    arrange(TotalNumber)%>% #zmieniamy kolejnosc rekordow
    as.data.frame()
}


all.equal(df_dplyr_3(Badges), df_sql_3(Badges))


#czwarte dplyr
df_dplyr_4 <- function(Users, Posts){
  AnsCount <- Posts%>% #tworzymy ramke pomocnicza
    filter(PostTypeId==2)%>% #filtrujemy wiersze
    group_by(ParentId)%>% #grupujemy
    summarise(AnswersCount = n()) #tworzymy nowa kolumne z zsumowana kiczba wierszy wzgledem grup
  
  PostAuth <- inner_join(Posts, AnsCount, by = c('Id' = 'ParentId')) #tworzymy kolejna ramke laczac ramke Posts i AnsCount wzgledem odpowiednich kol.
  
  dp <- inner_join(Users, PostAuth, by= c('AccountId' = 'OwnerUserId'))%>% #tworzymy ostateczna ramke
    group_by(AccountId, DisplayName, Location)%>% #grupujemy wzgl 3 kolumn
    summarise(AverageAnswersCount = mean(AnswersCount), .groups = 'drop')%>% #tworzymy nowa kolumne ze srednia z kolumny AnswersCount
    arrange(desc(AverageAnswersCount), AccountId)%>% #zmieniamy kolejnosc wierszy na odopwiednio malejaca i rosnaca wzgledem poszczegolnych kolumn
    as.data.frame()%>%
    head(10) #zwracamy 10 rekordow
}


all.equal(df_dplyr_4(Users, Posts), df_sql_4(Users, Posts))




#piate dplyr
df_sql_5(Posts, Votes)

df_dplyr_5 <- function(Posts, Votes){
  dp_tymczasowa <- Votes%>%
    filter(VoteTypeId %in% c(1, 2, 5))%>% #filtrujemy odpowiednio wiersze
    select(PostId, CreationDate)%>% #wybieramy kolumny
    mutate(CreationDate = substr(CreationDate, 1, 4))%>% #zmieniamy format kolumny by pokazywal rok
    mutate(VoteDate = ifelse(CreationDate == '2021' | CreationDate == '2020', 'new', 'old'))%>% #tworzymy nowa kolumne na podstawie dancyh z CreationDate
    group_by(PostId, VoteDate)%>% #gruoujemy po dwoch kolumnach
    summarise(Total = n(), .groups = 'drop')%>% #tworzymy nowa kolumne z podlicznymi wierszami
    mutate(NewVotes = ifelse(VoteDate == 'new', Total, 0))%>% #tworzymy dwie nowe kolumny zawierajace dane z kolumny Total gdy sa spelnione warunki
    mutate(OldVotes = ifelse(VoteDate == 'old', Total, 0))%>%
    group_by(PostId)%>% #grupujemy po PostId
    summarise(NewVotes = sum(NewVotes), OldVotes = sum(OldVotes), Votes = sum(Total))%>% #sumujemy zawartosci kolumn
    filter(NewVotes > OldVotes) -> VotesByAge #filtrujemy na podstawie warunku z polecenia i zapisujemy do nowej ramki
    
  dp <- Posts%>% #tworzymy nowa ramke an podstawie ramki posts
    select(Title, Id, CreationDate)%>% #wybieramy odpowiednie kolumny
    mutate(CreationDate = substr(CreationDate, 1, 10))%>% #zmieniamy format kolumny CreationDate
    rename(Date = CreationDate)%>% #zmienamy nazwe kolumny CreationDate
    filter(Title != '')%>% #filtrujemy odpowiednie wiersze
    inner_join(y = VotesByAge, by = c('Id' = 'PostId'))%>% #laczymy ze soba dwie ramki wzgledem kolumn Id i PostId
    select(Title, Id, Date, Votes)%>% #wybieramy koncowe kolumny
    arrange(-Votes)%>% #zmieniamy kolejnosc wierszy
    head(10) #zwracamy 10 rekordow
}

all.equal(df_dplyr_5(Posts, Votes), df_sql_5(Posts, Votes))




#------------------------------------------------
#rozwiazanie przy pomocy pakietu data.table

#pierwsze 
df_table_1 <- function(Tags){
  dt1 <- as.data.table(Tags) #tworzymy zmienna pomocnicza ktora bedzie w formacie data table
  dt1 <- dt1[, .(Count, TagName) #wybieramy kolumny
         ][Count>1000, #wybieramy interesuj¹ce nas wiersze
           ][order(-Count),] #zmieniamy kolejnosc na descending po Count
  dt1 <- as.data.frame(dt1)
}

all.equal(df_table_1(Tags), df_sql_1(Tags)) #porownujemy

#drugie
df_table_2 <- function(Users, Posts){
  Posts1 <- as.data.table(Posts) #zmieniamy na data.table
  Users1 <- as.data.table(Users) #zmieniamy na data.table
  dt2 <- Users1[ , .(Id, Location) #wybieramy interesujace nas kolumny
                 ][Location!= '', #filtrujemy te wiersze ktore spelniaja zalozenie
                   ][Posts1, on =.(Id = OwnerUserId), nomatch = FALSE #laczymy ze soba dwie ramki
                     ][,.(Count =.N), by = Location #liczymy interesujace nas wiersze i grupujemy po Lokalizacji
                       ][order(-Count),] #ustawiamy odpowiednia kolejnosc
  
  dt2 <- as.data.frame(head(dt2, 10)) #zwracamy 10 wierszy
}
all.equal(df_table_2(Users, Posts), df_sql_2(Users, Posts))



#trzecie 
df_table_3 <- function(Badges){
  Badges1 <- as.data.table(Badges) #zmieniamy format na data.table
  dt3 <- Badges1[Class == 1, .(Name, Date), #filtrujemy wiersze i wybieramy dwie kolumny
                 ][ , .(Year= substr(Date, 1, 4), Name),  #tworzymy nowa kolumne i grupujemy po Name
                    ][ , .(Number = .N), by = .(Name, Year) #liczymy liczbe rekordow grupujac po Name i Year
                       ][, .(TotalNumber= sum(Number)), by = Year #Tworzymy nowa kolumne, zawierajaca zsumowana kolumne Number po grupowaniu wzgledem Year
                         ][order(TotalNumber), ]#ustawiamy kolejnosc
  dt3 <- as.data.frame(dt3)
}


all.equal(df_table_3(Badges), df_sql_3(Badges))



#czwarte table

df_table_4 <- function(Users, Posts){
  Users1 <- as.data.table(Users)#zmienaimy typy 
  Posts1<- as.data.table(Posts)
  dt4 <- Posts1[PostTypeId==2, , by = ParentId#filtrujemy wiersze i grupujemy wzgl ParentId
                ][ , .(AnswerCount= .N), by= ParentId #tworzymy nowa kolumne i liczymy wiersze wzgl ParentId
                   ][Posts1, on=.(ParentId= Id), nomatch = FALSE #laczymy dwie ramki ze soba
                     ][Users1, on =.(OwnerUserId=AccountId), nomatch = FALSE #ponownie laczymy dwie ramki
                       ][, .(AverageAnswersCount = mean(AnswerCount)), by = .(DisplayName, Location, OwnerUserId) #tworzymy nowa kolumne
                         ][order(-AverageAnswersCount, OwnerUserId),#zmeinaimy kolejnosc
                           ][, head(.SD, 10)][, .(AccountId = OwnerUserId, DisplayName, Location, AverageAnswersCount)]#zwracamy 10 rekordow i wybrane kolumny
  dt4 <- as.data.frame(dt4)
}

all.equal(df_table_4(Users, Posts) , df_sql_4(Users, Posts))



#piate table
df_table_5 <- function(Posts, Votes){
  Post_tabelowe <- as.data.table(Posts)#zmieniamy format na data.table
  Votes_tabelowe <- as.data.table(Votes)
  
  dt <- Votes_tabelowe[VoteTypeId %in% c(1, 2, 5), .(PostId, Date = substr(CreationDate, 1, 4)) #filtrujemy wiersze, wybieramy i tworzymy nowa kolumne
                        ][, .(PostId, VoteDate = ifelse(Date =='2021' | Date == '2020', 'new', 'old'))#wybieramry i tworzymy nowa kolumne na podstawie odpowiednich warunkow
                          ][, .(Total = .N), by = .(PostId, VoteDate)#tworzymy nowa kolumne ze zliczonymi wierszami wzgl ospowiednich grup
                            ][, .(PostId, VoteDate, Total, 
                 OldVotes = ifelse(VoteDate == 'old', Total, 0), NewVotes = ifelse(VoteDate == 'new', Total, 0)) #tworzymy nowe kolumny w zaleznoscu od zawartosci kolumny VoteDate
                 ][, .(Total, NewVotes = sum(NewVotes), OldVotes = sum(OldVotes), Votes = sum(Total)), by = PostId
                   ][NewVotes > OldVotes, #filtrujemy   wiersze
                     ][Post_tabelowe, on = .(PostId = Id), nomatch = FALSE] -> VotesByAge #laczymy ramki ze soba
  
  dt5 <- VotesByAge[, .(Title, Id = PostId, Date = substr(CreationDate, 1, 10), Votes) #wybieramy odpowiednie kolumny i tworzymy
                    ][Title != '',  #wyrzucamy puste wiersze w kolumnie title
                      ][order(-Votes),]#zmeiniamy koljenosc
  dt5 <- as.data.frame(dt5)
  dt5 <- head(dt5, 10)
}
all.equal(df_table_5(Posts, Votes), df_sql_5(Posts, Votes))



