DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28);
INSERT INTO Customers VALUES  ('Stephanie','Cox','Management abcd defg','Bachelors',33);
INSERT INTO Customers VALUES  ('Peter','Nara','Skilled Manual','Graduate Degree',26);
INSERT INTO Customers VALUES  ('Latoya','Shen','Professional','Graduate Degree',25);
INSERT INTO Customers VALUES  ('Joshua','Lee','Professional','Partial College',26);
INSERT INTO Customers VALUES  ('Edward','Hernandez','Skilled Manual','High School',36);
INSERT INTO Customers VALUES  ('Dalton','Wood','Professional','Partial College',42);
INSERT INTO Customers VALUES  ('Christine','Nara','Skilled Manual','Partial College',33);
INSERT INTO Customers VALUES  ('Cameron','Rodriguez','Professional','Partial College',28);
INSERT INTO Customers VALUES  ('Angel','Stewart','Professional','Partial College',46);
INSERT INTO Customers VALUES  ('Apple','','Skilled Manual','Bachelors',28);
INSERT INTO Customers VALUES  (NULL,'why','Professional','Partial College',38);

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
Customers | summarize count() by bin(Age, 10) | order by count() ASC;

-- The following does not work
-- arg_max()
-- arg_min()
