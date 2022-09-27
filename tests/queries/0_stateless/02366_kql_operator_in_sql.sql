DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES  ('Theodore','Diaz','Skilled Manual','Bachelors',28),('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);
Select '-- test kql operator in sql --' ;
select * from kql(Customers | where FirstName !in ('Peter', 'Latoya'));
select * from kql(Customers | where FirstName !contains 'Pet');
select * from kql(Customers | where FirstName !contains_cs 'Pet');
select * from kql(Customers | where FirstName !endswith 'ter');
select * from kql(Customers | where FirstName !endswith_cs 'ter');
select * from kql(Customers | where FirstName != 'Peter');
select * from kql(Customers | where FirstName !has 'Peter');
select * from kql(Customers | where FirstName !has_cs 'peter');
select * from kql(Customers | where FirstName !hasprefix 'Peter');
select * from kql(Customers | where FirstName !hasprefix_cs 'Peter');
select * from kql(Customers | where FirstName !hassuffix 'Peter');
select * from kql(Customers | where FirstName !hassuffix_cs 'Peter');
select * from kql(Customers | where FirstName !startswith 'Peter');
select * from kql(Customers | where FirstName !startswith_cs 'Peter');
DROP TABLE IF EXISTS Customers;
