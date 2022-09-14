DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

-- INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28);
-- INSERT INTO Customers VALUES  ('Stephanie','Cox','Management abcd defg','Bachelors',33);
-- INSERT INTO Customers VALUES  ('Peter','Nara','Skilled Manual','Graduate Degree',26);
-- INSERT INTO Customers VALUES  ('Latoya','Shen','Professional','Graduate Degree',25);
-- INSERT INTO Customers VALUES  ('Joshua','Lee','Professional','Partial College',26);
-- INSERT INTO Customers VALUES  ('Edward','Hernandez','Skilled Manual','High School',36);
-- INSERT INTO Customers VALUES  ('Dalton','Wood','Professional','Partial College',42);
-- INSERT INTO Customers VALUES  ('Christine','Nara','Skilled Manual','Partial College',33);
-- INSERT INTO Customers VALUES  ('Cameron','Rodriguez','Professional','Partial College',28);
-- INSERT INTO Customers VALUES  ('Angel','Stewart','Professional','Partial College',46);
-- INSERT INTO Customers VALUES  ('Apple','','Skilled Manual','Bachelors',28);
-- INSERT INTO Customers VALUES  (NULL,'why','Professional','Partial College',38);

INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28),('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Joshua','Lee','Professional','Partial College',26),('Edward','Hernandez','Skilled Manual','High School',36),('Dalton','Wood','Professional','Partial College',42),('Christine','Nara','Skilled Manual','Partial College',33),('Cameron','Rodriguez','Professional','Partial College',28),('Angel','Stewart','Professional','Partial College',46),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);


Select '-- test summarize --' ;
set dialect='kusto';
Customers | summarize count(), min(Age), max(Age), avg(Age), sum(Age);
Customers | summarize count(), min(Age), max(Age), avg(Age), sum(Age) by Occupation;
Customers | summarize countif(Age>40) by Occupation;
Customers | summarize MyMax = maxif(Age, Age<40) by Occupation;
Customers | summarize MyMin = minif(Age, Age<40) by Occupation;
Customers | summarize MyAvg = avgif(Age, Age<40) by Occupation;
Customers | summarize MySum = sumif(Age, Age<40) by Occupation;
Customers | summarize dcount(Education);
Customers | summarize dcountif(Education, Occupation=='Professional');
Customers | summarize count_ = count() by bin(Age, 10) | order by count_ asc;

-- make_list()
Customers | summarize f_list = make_list(Education) by Occupation;
Customers | summarize f_list = make_list(Education, 2) by Occupation;
-- make_list_if()
Customers | summarize f_list = make_list_if(FirstName, Age>30) by Occupation;
Customers | summarize f_list = make_list_if(FirstName, Age>30, 1) by Occupation;
-- make_set()
Customers | summarize f_list = make_set(Education) by Occupation;
Customers | summarize f_list = make_set(Education, 2) by Occupation;
-- make_set_if()
Customers | summarize f_list = make_set_if(Education, Age>30) by Occupation;
Customers | summarize f_list = make_set_if(Education, Age>30, 1) by Occupation;
-- stdev()
Customers | project Age | summarize stdev(Age);
-- stdevif()
Customers | project Age | summarize stdevif(Age, Age%2==0);
-- binary_all_and
Customers | project Age | where Age > 40 | summarize binary_all_and(Age);
-- binary_all_or
Customers | project Age | where Age > 40 | summarize binary_all_or(Age);
-- binary_all_xor
Customers | project Age | where Age > 40 | summarize binary_all_xor(Age);

-- TODO:
Customers | project Age | summarize percentile(Age, 95);
Customers | project Age | summarize percentiles(Age, 5, 50, 95);
Customers | project Age | summarize percentiles(Age, 5, 50, 95)[1];
-- Customers | summarize w=count() by AgeBucket=bin(Age, 5) | summarize percentilew(AgeBucket, w, 75); -- expect 35
-- Customers | summarize w=count() by AgeBucket=bin(Age, 5) | summarize percentilesw(AgeBucket, w, 50, 75, 99.9); -- expect 25,35,45

-- arg_max()
-- arg_min()
-- make_list_with_nulls()
-- Customers | sort by FirstName | summarize count(Education) by Occupation;