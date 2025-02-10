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

set dialect='kusto';
print '-- test Query only has table name: --';
Customers;

print '-- Query has Column Selection --';
Customers | project FirstName,LastName,Occupation;

print '-- Query has limit --';
Customers | project FirstName,LastName,Occupation | take 5;
Customers | project FirstName,LastName,Occupation | limit 5;

print '-- Query has second limit with bigger value --';
Customers | project FirstName,LastName,Occupation | take 5 | take 7;

print '-- Query has second limit with smaller value --';
Customers | project FirstName,LastName,Occupation | take 5 | take 3;

print '-- Query has second Column selection --';
Customers | project FirstName,LastName,Occupation | take 3 | project FirstName,LastName;

print '-- Query has second Column selection with extra column --';
Customers| project FirstName,LastName,Occupation | take 3 | project FirstName,LastName,Education;-- { serverError UNKNOWN_IDENTIFIER }

print '-- Query with desc sort --';
Customers | project FirstName | take 5 | sort by FirstName desc;
Customers | project Occupation | take 5 | order by Occupation desc;

print '-- Query with asc sort --';
Customers | project Occupation | take 5 | sort by Occupation asc;

print '-- Query with sort (without keyword asc desc) --';
Customers | project FirstName | take 5 | sort by FirstName;
Customers | project Occupation | take 5 | order by Occupation;

print '-- Query with sort 2 Columns with different direction --';
Customers | project FirstName,LastName,Occupation | take 5 | sort by Occupation asc, LastName desc;

print '-- Query with second sort --';
Customers | project FirstName,LastName,Occupation | take 5 | sort by Occupation desc |sort by Occupation asc, LastName desc;

print '-- Test String Equals (==) --';
Customers | project FirstName,LastName,Occupation | where Occupation == 'Skilled Manual';

print '-- Test String Not equals (!=) --';
Customers | project FirstName,LastName,Occupation | where Occupation != 'Skilled Manual';

print '-- Test Filter using a list (in) --';
Customers | project FirstName,LastName,Occupation,Education | where Education in  ('Bachelors','High School');

print '-- Test Filter using a list (!in) --';
set dialect='kusto';
Customers | project FirstName,LastName,Occupation,Education | where Education !in  ('Bachelors','High School');

print '-- Test Filter using common string operations (contains_cs) --';
Customers | project FirstName,LastName,Occupation,Education | where Education contains_cs 'Coll';

print '-- Test Filter using common string operations (startswith_cs) --';
Customers | project FirstName,LastName,Occupation,Education | where Occupation startswith_cs 'Prof';

print '-- Test Filter using common string operations (endswith_cs) --';
Customers | project FirstName,LastName,Occupation,Education | where FirstName endswith_cs 'a';

print '-- Test Filter using numerical equal (==) --';
Customers | project FirstName,LastName,Occupation,Education,Age | where Age == 26;

print '-- Test Filter using numerical great and less (> , <) --';
Customers | project FirstName,LastName,Occupation,Education,Age | where Age > 30 and Age < 40;

print '-- Test Filter using multi where --';
Customers | project FirstName,LastName,Occupation,Education,Age | where Age > 30 | where Occupation == 'Professional';

print '-- Complex query with unknown function --';
hits | where CounterID == 62 and EventDate >= '2013-07-14' and EventDate <= '2013-07-15' and IsRefresh == 0 and DontCountHits == 0 | summarize count() by d=bin(poopoo(EventTime), 1m) | order by d | limit 10; -- { clientError UNKNOWN_FUNCTION }

print '-- Missing column in front of startsWith --';
StormEvents | where startswith "W" | summarize Count=count() by State; -- { clientError SYNTAX_ERROR }

SET max_query_size = 55;
SET dialect='kusto';
Customers | where Education contains 'degree' | order by LastName; -- { serverError SYNTAX_ERROR }
SET max_query_size=262144;
