library(foreign)
library(lubridate)
library(stringr)
library(magrittr)
library(ggplot2)
library(dplyr)
library(tidyr)


#read tables
osha <- read.dbf("osha.DBF")
info <- read.dbf("optinfo.DBF")
accid <- read.dbf("accid.DBF")
admpay <- read.dbf("admpay.DBF")
debt <- read.dbf("debt.DBF")
hazsub <- read.dbf("hazsub.DBF")
history <- read.dbf("history.DBF")
prog <- read.dbf("prog.DBF")
relact <- read.dbf("relact.DBF")
viol <- read.dbf("viol.DBF")


## check if a column has all NA in it, it's meaningless. We can drop it
indi = rep(0,ncol(osha))
for(i in 1: ncol(osha)){indi[i] = sum(!is.na(osha[,i]))}
which(indi==0)
## [1] 4, 9

tidyosha = osha[,-c(which(indi==0))]
rm(indi, osha)

#glimpse
head(tidyosha)
dim(tidyosha)
#[1] 80445    90

##check column head
colnames(tidyosha)
##colmns all are variable names

#check meaning of some columns in osha
#Since we are working on the most dangerous places in MA, delete state colomn
#We can also delete column sitezip
drop <- c("SITESTATE", "SITEZIP")
tidyosha = tidyosha[,!(names(tidyosha) %in% drop)]



#check level columns which that most of the observations are NA or 0
head(tidyosha)
table(tidyosha$CONTFLAG)#meaningless since 80445 observation only two sample has value in this column
#1 9 
#1 1 

table(tidyosha$OWNERCODE) #since 80223 oberservation are 0, delete
table(tidyosha$OPTREPTNO) #meaningless
table(tidyosha$CATSICGDE) #meaningless since 78727 are 0
# since we delete CASTICGDE, CATSICINSP is also meaningless
table(tidyosha$EMPCOUNT) #meaningless
table(tidyosha$EMPCOVERED) #meaningless
#delete colomn PENDUDT since it's the convert version of PENDUDATE

#remove all the meaningless columns
drop1 <- c("CONTFLAG", "OWNERCODE","OPTREPTNO","CATSICGDE","CATSICINSP","EMPCOUNT","EMPCOUNT","EMPCOVERED","PENDUDATE")
tidyosha = tidyosha[,!(names(tidyosha) %in% drop1)]


# some date columns have the same meaning, except for different form
# find those columns, delete the one which is not in form YYYY-MM-DD
open1 <- select(tidyosha, matches("OPEN"))
lstr1 <- select(tidyosha, matches("LSTR"))
frst1 <- select(tidyosha, matches("FRST"))
mod1 <- select(tidyosha, matches("MOD"))
close1 <- select(tidyosha, matches("CLOSE"))
pendu1 <-select(tidyosha, matches("PENDU"))
ftadu1 <- select(tidyosha, matches("FTADU"))


drop2 <- c("OPENDATE", "LSTREENTR", "FRSTDENY", "FRSTCONTST")
tidyosha = tidyosha[,!(names(tidyosha) %in% drop2)]




########  Accid
label1 <- read.dbf("lookups/acc.dbf")
if(sum(accid$SITESTATE=="MA") == dim(accid)[1]){accid %<>% select(-SITESTATE)}
dim(label1)

sum(label1$CATEGORY=="PART-BODY")
parts <- label1[(label1$CATEGORY== "PART-BODY"),]
dim(parts)

parts <- select(parts, CODE, VALUE)
head(parts)

colnames(parts) <- c("BODYPART", "BODYPART_VALUE")
str(parts)
accid_1 <- left_join(accid, parts, by="BODYPART")

#since we are looking for the most dangerous place to work, occupation is also important
#add a column in accid of occupation
lable2 <- read.dbf("lookups/occ.dbf")
if(sum(lable2$STATE=="MA") == dim(lable2)[1]) {lable2 %<>% select(-STATE)}
dim(lable2)

colnames(lable2) <- c("OCC_CODE", "OCCUPATION")
accid_clear <- left_join(accid_1, lable2, by = "OCC_CODE")

# decode column NATURE

lable3 <- read.dbf("lookups/acc.dbf")
nature_inj <- lable3[(lable3$CATEGORY == "NATUR-INJ"),]
dim(nature_inj)

colnames(nature_inj) <- c("NA-INJ", "NATURE","NATURE_VALUE")
accid_clear <- left_join(accid_clear, nature_inj, by = "NATURE")



#decode column SOURCE

souc_inj <- lable3[(lable3$CATEGORY == "SOURC-INJ"),]
colnames(souc_inj) <- c("SOURC-INJ","SOURCE","SOURCE_VALUE")
accid_clear <- left_join(accid_clear, souc_inj, by = "SOURCE")

#decode column EVENT
event <- lable3[(lable3$CATEGORY == "EVENT-TYP"),]
colnames(event) <- c("EVENT-TYP","EVENT","EVENT_VALUE")
accid_clear <- left_join(accid_clear, event, by = "EVENT")

#decode ENVIRON	
environ <- lable3[(lable3$CATEGORY == "ENVIR-FAC"),]
colnames(environ) <- c("ENVIR-FAC","ENVIRON","ENVIRON_VALUE")
accid_clear <- left_join(accid_clear, environ, by = "ENVIRON")

#decode HAZSUB
haz <- read.dbf("lookups/hzs.dbf")
colnames(haz) <- c("HAZSUB","HAZSUB_VALUE")
accid_clear <- left_join(accid_clear, haz, by = "HAZSUB")


#### delete meaningless columns
drop <- c("SOURC-INJ","SOURCE","NA-INJ", "NATURE","OCC_CODE","BODYPART","EVENT-TYP","EVENT","ENVIR-FAC","ENVIRON","HAZSUB", "HUMAN","TASK")
accid_clear = accid_clear[,!(names(accid_clear) %in% drop)]
# done with accid form


#### combine accid and osha
tidyosha <- left_join(accid_clear, tidyosha, by = "ACTIVITYNO")

#delete ACCID_
tidyosha = tidyosha[,!(names(tidyosha) %in% c("ACCID_"))]

## find duplicated data
library(data.table)

b <- colnames(tidyosha[1:ncol(tidyosha)]) 

a <- data.table(tidyosha, key= b)
dupli_rows <- a[unique(a[duplicated(a)]),which = T]
length(dupli_rows)

tidyosha = tidyosha[-dupli_rows,]


##decote SITECITY
sitecity <- read.dbf("lookups/scc.dbf")
sitecity_1 <- sitecity[(sitecity$STATE=="MA"),]
colnames(sitecity_1) <- c("TYPE", "STATE","SITECNTY","SITECITY","SITECITY_VALUE")
tidyosha <- left_join(sitecity_1, tidyosha, by = "SITECITY")

#delete meaningless columns

drop <- c("TYPE", "STATE","SITECNTY","SITECITY")
tidyosha = tidyosha[,!(names(tidyosha) %in% drop)]






####plot

#barplot of SITECITY and ACTIVITYNO, because there are too any observations, choose the ones that are greater than 300000000
dangerous<- filter(tidyosha, ACTIVITYNO>300000000)

#histogram of 50 companies with the most activity numbers in alphabetical order of companies' names
# the 50th activity number is 306806944
dangerous_companies <- subset(dangerous, subset = ACTIVITYNO > 306806944)
dangerous_companies <- within(dangerous_companies,
                              ESTABNAME <- factor(ESTABNAME,
                                                  levels = names(sort(table(ESTABNAME), decreasing = TRUE))))
c <- ggplot(dangerous_companies, aes(ESTABNAME, ACTIVITYNO))
c + geom_histogram(binwidth = 2, stat = "identity" , aes(fill = SITECITY_VALUE)) + coord_flip()

#bar plot of job title and activity number in terms of sitecity
dangerous_cities = subset(dangerous, subset = OCCUPATION != "NA" )
d <- ggplot(dangerous_cities, aes(JOBTITLE, ACTIVITYNO))
d + geom_bar(stat = "identity", aes(fill = SITECITY_VALUE))

#plot of activity numbers in BOSTON in terms of their job title and Latest date activity applied against record.
BOSTON = subset(tidyosha, subset = SITECITY_VALUE == "BOSTON")
p <- ggplot(BOSTON, aes(x = MOD_DATE, y = ACTIVITYNO))
p + geom_point(aes(colour = factor (JOBTITLE)), size = 1)
