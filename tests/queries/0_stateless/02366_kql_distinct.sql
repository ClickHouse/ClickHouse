DROP TABLE IF EXISTS Customers;
CREATE TABLE Customers
(    
    FirstName Nullable(String),
    LastName String, 
    Occupation String,
    Education String,
    Age Nullable(UInt8)
) ENGINE = Memory;

INSERT INTO Customers VALUES ('Theodore','Diaz','Skilled Manual','Bachelors',28), ('Stephanie','Cox','Management abcd defg','Bachelors',33),('Peter','Nara','Skilled Manual','Graduate Degree',26),('Latoya','Shen','Professional','Graduate Degree',25),('Apple','','Skilled Manual','Bachelors',28),(NULL,'why','Professional','Partial College',38);

-- The reference relies on `distinct` keeping the input order, which a spilled DISTINCT does not
-- (CI randomizes the external DISTINCT thresholds).
set max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 0;
set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';

print '--  distinct * --';
Customers | distinct *;

print '--  distinct one column --';
Customers | distinct Occupation;

print '--  distinct two column --';
Customers | distinct Occupation, Education;

print '--  distinct with where --';
-- [removed in the KQL rewrite] Syntax error in KQL query at position 1: 'where' is a pipeline operator, so it must follow a '|', found 'where'
-- Customers where Age <30 | distinct Occupation, Education;

print '--  distinct with where, order --';
Customers |where Age <30 | order by Age| distinct Occupation, Education;
