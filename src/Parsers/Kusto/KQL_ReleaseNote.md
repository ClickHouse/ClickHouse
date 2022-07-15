# KQL implemented features.


# July 17, 2022

## Renamed dialect from sql_dialect to dialect

`set sql_dialect='clickhouse'`  
`set sql_dialect='kusto'`  
`set sql_dialect='kusto_auto'`

## string functions
- **support subquery for `in` orerator** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/in-cs-operator)  
 (subquery need to be wraped with bracket inside bracket)

    `Customers | where Age in ((Customers|project Age|where Age < 30))`
 Note: case-insensitive not supported yet
- **has_all**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-all-operator)  
    `Customers|where Occupation has_any ('Skilled','abcd')`  
     note : subquery not supported yet
- **has _any**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-anyoperator)  
    `Customers|where Occupation has_all ('Skilled','abcd')`  
    note : subquery not supported yet
- **countof**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countoffunction)  
   `Customers | project countof('The cat sat on the mat', 'at')`  
   `Customers | project countof('The cat sat on the mat', 'at', 'normal')`  
   `Customers | project countof('The cat sat on the mat', 'at', 'regex')`
- **extract**  ( https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractfunction)  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 0, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 1, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 3, 'The price of PINEAPPLE ice cream is 20')`  
`Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 20', typeof(real))` 

- **extract_all** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractallfunction)  

    `Customers | project extract_all('(\\w)(\\w+)(\\w)','The price of PINEAPPLE ice cream is 20')`  
    note:  captureGroups not supported yet

- **split** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction)  
    `Customers | project split('aa_bb', '_')`  
    `Customers | project split('aaa_bbb_ccc', '_', 1)`  
    `Customers | project split('', '_')`  
    `Customers | project split('a__b', '_')`  
    `Customers | project split('aabbcc', 'bb')`  

- **strcat_delim**  (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-delimfunction)  
    `Customers | project strcat_delim('-', '1', '2', 'A') , 1s)`  
    `Customers | project strcat_delim('-', '1', '2', strcat('A','b'))`  
    note: only support string now.

- **indexof** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/indexoffunction)  
    `Customers | project indexof('abcdefg','cde')`  
    `Customers | project indexof('abcdefg','cde',2)`  
    `Customers | project indexof('abcdefg','cde',6)`  
    note: length and occurrence not supported yet




# July 4, 2022

## sql_dialect

- default is `clickhouse`  
    `set sql_dialect='clickhouse'`
- only process kql  
    `set sql_dialect='kusto'`
- process both kql and CH sql  
    `set sql_dialect='kusto_auto'`
## KQL() function

 - create table  
  `CREATE TABLE kql_table4 ENGINE = Memory AS select *, now() as new_column From kql(Customers | project LastName,Age);`  
   verify the content of `kql_table`  
    `select * from kql_table`
   
 - insert into table  
    create a tmp table:
    ```
    CREATE TABLE temp
    (    
        FirstName Nullable(String),
        LastName String, 
        Age Nullable(UInt8)
    ) ENGINE = Memory;
    ```
    `INSERT INTO temp select * from kql(Customers|project FirstName,LastName,Age);`  
    verify the content of `temp`   
        `select * from temp`

 - Select from kql()  
    `Select * from kql(Customers|project FirstName)`

## KQL operators:
 - Tabular expression statements  
    `Customers`
 - Select Column  
    `Customers | project FirstName,LastName,Occupation`
 - Limit returned results  
    `Customers | project FirstName,LastName,Occupation | take 1 | take 3`
 - sort, order  
    `Customers | order by Age desc , FirstName asc`
 - Filter   
    `Customers | where Occupation == 'Skilled Manual'`
 - summarize  
    `Customers |summarize  max(Age) by Occupation`

## KQL string operators and functions
 - contains  
    `Customers |where Education contains  'degree'`
 - !contains  
    `Customers |where Education !contains  'degree'`
 - contains_cs  
    `Customers |where Education contains  'Degree'`
 - !contains_cs  
    `Customers |where Education !contains  'Degree'`
 - endswith  
    `Customers | where FirstName endswith 'RE'`
 - !endswith  
     `Customers | where !FirstName endswith 'RE'`
 - endswith_cs  
 `Customers | where FirstName endswith_cs  're'`
 - !endswith_cs  
  `Customers | where FirstName !endswith_cs  're'`
 - ==  
    `Customers | where Occupation == 'Skilled Manual'`
 - !=  
    `Customers | where Occupation != 'Skilled Manual'`
 - has  
    `Customers | where Occupation has 'skilled'`
 - !has  
    `Customers | where Occupation !has 'skilled'`
 - has_cs  
    `Customers | where Occupation has 'Skilled'`
 - !has_cs  
    `Customers | where Occupation !has 'Skilled'`
 - hasprefix  
    `Customers | where Occupation hasprefix_cs 'Ab'`
 - !hasprefix  
    `Customers | where Occupation !hasprefix_cs 'Ab'`
 - hasprefix_cs  
    `Customers | where Occupation hasprefix_cs 'ab'`
 - !hasprefix_cs  
    `Customers | where Occupation! hasprefix_cs 'ab'`
 - hassuffix  
    `Customers | where Occupation hassuffix 'Ent'`
 - !hassuffix  
    `Customers | where Occupation !hassuffix 'Ent'`
 - hassuffix_cs  
    `Customers | where Occupation hassuffix 'ent'`
 - !hassuffix_cs  
    `Customers | where Occupation hassuffix 'ent'`
 - in  
    `Customers |where Education in ('Bachelors','High School')`
 - !in  
    `Customers |  where Education !in  ('Bachelors','High School')`
 - matches regex  
    `Customers | where FirstName matches regex 'P.*r'`
 - startswith  
    `Customers | where FirstName startswith 'pet'`
 - !startswith  
    `Customers | where FirstName !startswith 'pet'`
 - startswith_cs  
    `Customers | where FirstName startswith_cs 'pet'`
 - !startswith_cs  
     `Customers | where FirstName !startswith_cs 'pet'`

 - base64_encode_tostring()  
 `Customers | project base64_encode_tostring('Kusto1') | take 1`
 - base64_decode_tostring()  
 `Customers | project base64_decode_tostring('S3VzdG8x') | take 1`
  - isempty()  
 `Customers | where  isempty(LastName)`
 - isnotempty()  
  `Customers | where  isnotempty(LastName)`
 - isnotnull()  
  `Customers | where  isnotnull(FirstName)`
 - isnull()  
 `Customers | where  isnull(FirstName)`
 - url_decode()  
 `Customers | project url_decode('https%3A%2F%2Fwww.test.com%2Fhello%20word') | take 1`
 - url_encode()  
    `Customers | project url_encode('https://www.test.com/hello word') | take 1`
 - substring()  
    `Customers | project name_abbr = strcat(substring(FirstName,0,3), ' ', substring(LastName,2))`
 - strcat()  
    `Customers | project name = strcat(FirstName, ' ', LastName)`
 - strlen()  
    `Customers | project FirstName, strlen(FirstName)`
 - strrep()  
    `Customers | project strrep(FirstName,2,'_')`
 - toupper()  
    `Customers | project toupper(FirstName)`
 - tolower()  
    `Customers | project tolower(FirstName)`

 ## Aggregate Functions
 - arg_max()
 - arg_min()
 - avg()
 - avgif()
 - count()
 - countif()
 - max()
 - maxif()
 - min()
 - minif()
 - sum()
 - sumif()
 - dcount()
 - dcountif()
 - bin