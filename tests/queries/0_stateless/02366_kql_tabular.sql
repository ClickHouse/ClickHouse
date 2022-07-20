DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28), ('Stephanie','Cox','Management','Bachelors',33), ('Peter','Nara','Skilled Manual','Graduate Degree',26), ('Latoya','Shen','Professional','Graduate Degree',25), ('Joshua','Lee','Professional','Partial College',26), ('Edward','Hernandez','Skilled Manual','High School',36), ('Dalton','Wood','Professional','Partial College',42), ('Christine','Nara','Skilled Manual','Partial College',33), ('Cameron','Rodriguez','Professional','Partial College',28), ('Angel','Stewart','Professional','Partial College',46);

set dialect='clickhouse';
Select '-- test Query only has table name: --';
set dialect='kusto';
Customers;

set dialect='clickhouse';
Select '-- Query has Column Selection --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation;

set dialect='clickhouse';
Select '-- Query has limit --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | take 5;
Customers | project FirstName,LastName,Occupation | limit 5;

set dialect='clickhouse';
Select '-- Query has second limit with bigger value --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | take 5 | take 7;

set dialect='clickhouse';
Select '-- Query has second limit with smaller value --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | take 5 | take 3;

set dialect='clickhouse';
Select '-- Query has second Column selection --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | take 3 | project FirstName,LastName;

set dialect='clickhouse';
Select '-- Query has second Column selection with extra column --';
set dialect='kusto';
Customers| project FirstName,LastName,Occupation | take 3 | project FirstName,LastName,Education;

set dialect='clickhouse';
Select '-- Query with desc sort --';
set dialect='kusto';
Customers | project FirstName | take 5 | sort by FirstName desc;
Customers | project Occupation | take 5 | order by Occupation desc;

set dialect='clickhouse';
Select '-- Query with asc sort --';
set dialect='kusto';
Customers | project Occupation | take 5 | sort by Occupation asc;

set dialect='clickhouse';
Select '-- Query with sort (without keyword asc desc) --';
set dialect='kusto';
Customers | project FirstName | take 5 | sort by FirstName;
Customers | project Occupation | take 5 | order by Occupation;

set dialect='clickhouse';
Select '-- Query with sort 2 Columns with different direction --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | take 5 | sort by Occupation asc, LastName desc;

set dialect='clickhouse';
Select '-- Query with second sort --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | take 5 | sort by Occupation desc |sort by Occupation asc, LastName desc;

set dialect='clickhouse';
Select '-- Test String Equals (==) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | where Occupation == 'Skilled Manual';

set dialect='clickhouse';
Select '-- Test String Not equals (!=) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation | where Occupation != 'Skilled Manual';

set dialect='clickhouse';
Select '-- Test Filter using a list (in) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education | where Education in  ('Bachelors','High School');

set dialect='clickhouse';
Select '-- Test Filter using a list (!in) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education | where Education !in  ('Bachelors','High School');

set dialect='clickhouse';
Select '-- Test Filter using common string operations (contains_cs) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education | where Education contains_cs 'Coll';

set dialect='clickhouse';
Select '-- Test Filter using common string operations (startswith_cs) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education | where Occupation startswith_cs 'Prof';

set dialect='clickhouse';
Select '-- Test Filter using common string operations (endswith_cs) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education | where FirstName endswith_cs 'a';

set dialect='clickhouse';
Select '-- Test Filter using numerical equal (==) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education,Age | where Age == 26;

set dialect='clickhouse';
Select '-- Test Filter using numerical great and less (> , <) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education,Age | where Age > 30 and Age < 40;

set dialect='clickhouse';
Select '-- Test Filter using multi where --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education,Age | where Age > 30 | where Occupation == 'Professional';
