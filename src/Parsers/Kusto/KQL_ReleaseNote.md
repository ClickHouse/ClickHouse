## KQL implemented features  

# October 9, 2022  

## operator  
- [distinct](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/distinctoperator)  
   `Customers | distinct *`  
   `Customers | distinct Occupation`  
   `Customers | distinct Occupation, Education`  
   `Customers | where Age <30 | distinct Occupation, Education`  
   `Customers | where Age <30 | order by Age| distinct Occupation, Education`  

## String functions  
- [reverse](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/reversefunction)  
   `print reverse(123)`  
   `print reverse(123.34)`  
   `print reverse('clickhouse')`  
   `print reverse(3h)`  
   `print reverse(datetime(2017-1-1 12:23:34))`  

- [parse_command_line](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-command-line)  
   `print parse_command_line('echo \"hello world!\" print$?', \"Windows\")`  
  
- [parse_csv](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parsecsvfunction)  
  `print result=parse_csv('aa,b,cc')`  
  `print result_multi_record=parse_csv('record1,a,b,c\nrecord2,x,y,z')`  

- [parse_json](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parsejsonfunction)  
   `print parse_json( dynamic([1, 2, 3]))`  
   `print parse_json('{"a":123.5, "b":"{\\"c\\":456}"}')`  

- [extract_json](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extractjsonfunction)  
   `print extract_json( "$.a" , '{"a":123, "b":"{\\"c\\":456}"}' , typeof(int))`  

- [parse_version](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-versionfunction)  
 `print parse_version('1')`  
 `print parse_version('1.2.3.40')`  

## Bug fixed
- [correct array index in expression](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1474)  
   array index should start with 0  
- [Summarize should generate alias or use correct columns](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1303)
   - if bin is used , the column should be in select list if no alias include  
   - if no column included in aggregate functions,  ( like count() ), should has alias with fun name + '_',e.g  count_  
   - if column name included in aggregate functions, should have fun name + "_" + column name , like count(Age) -> count_Age  
   - if argument of an aggregate functions is an exprision, Columns1 ... Columnsn should be used as alias  
      ```
      Customers | summarize count() by bin(Age, 10)  
      ┌─Age─┬─count_─┐
      │  40 │      2 │
      │  20 │      6 │
      │  30 │      4 │
      └─────┴────────┘
      Customers | summarize count(Age) by bin(Age, 10)  
      ┌─Age─┬─count_Age─┐
      │  40 │         2 │
      │  20 │         6 │
      │  30 │         4 │
      └─────┴───────────┘
      Customers | summarize count(Age+1) by bin(Age+1, 10)
      ┌─Columns1─┬─count_─┐
      │       40 │      2 │
      │       20 │      6 │
      │       30 │      4 │
      └──────────┴────────┘
      ```
- [extend doesn't replace existing columns](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1246)  

- [throw exception if use quoted string as alias](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1470)  

- [repeat() doesn't work with count argument as negative value](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1368)  

- [substring() doesn't work right with negative offsets](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1336)  
- [endofmonth() doesn't return correct result](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1370)  

- [split() outputs array instead of string](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1343)  

- [split() returns empty string when arg goes out of bound](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1328)  

- [split() doesn't work with negative index](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1325)  


# September 26, 2022
## Bug fixed :  
["select * from kql" results in syntax error](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1119)  
[Parsing ipv4 with arrayStringConcat throws exception](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1259)  
[CH Client crashes on invalid function name](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1266)  
[extract() doesn't work right with 4th argument i.e typeof()](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1327)  
[parse_ipv6_mask return incorrect results](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1050)  
[timespan returns wrong output in seconds](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1275)  
[timespan doesn't work for nanoseconds and tick](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1298)  
[totimespan() doesn't work for nanoseconds and tick timespan unit](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1301)  
[data types should throw exception in certain cases](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1112)  
[decimal does not support scientific notation](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1197)  
[extend statement causes client core dumping](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1260)  
[extend crashes with array sorting](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1247)  
[Core dump happens when WHERE keyword doesn't follow field name](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1335)  
[Null values are missing in the result of `make_list_with_nulls'](https://github.ibm.com/ClickHouse/issue-repo/issues/1009)  
[trim functions use non-unique aliases](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1111)  
[format_ipv4_mask returns incorrect mask value](https://zenhub.ibm.com/workspaces/clickhouse-project-61250df53aaf060db4e08052/issues/clickhouse/issue-repo/1039)  

# September 12, 2022
## Extend operator
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extendoperator  
`T | extend T | extend duration = endTime - startTime`  
`T | project endTime, startTime | extend duration = endTime - startTime`
## Array functions
- [array_reverse](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array-reverse-function)  
   `print array_reverse(dynamic(["this", "is", "an", "example"])) == dynamic(["example","an","is","this"])`  

- [array_rotate_left](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_leftfunction)  
   `print array_rotate_left(dynamic([1,2,3,4,5]), 2) == dynamic([3,4,5,1,2])`  
   `print array_rotate_left(dynamic([1,2,3,4,5]), -2) == dynamic([4,5,1,2,3])`  

- [array_rotate_right](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_rightfunction)  
   `print array_rotate_right(dynamic([1,2,3,4,5]), -2) == dynamic([3,4,5,1,2])`  
   `print array_rotate_right(dynamic([1,2,3,4,5]), 2) == dynamic([4,5,1,2,3])`  

- [array_shift_left](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_leftfunction)  
   `print array_shift_left(dynamic([1,2,3,4,5]), 2) == dynamic([3,4,5,null,null])`  
   `print array_shift_left(dynamic([1,2,3,4,5]), -2) == dynamic([null,null,1,2,3])`  
   `print array_shift_left(dynamic([1,2,3,4,5]), 2, -1) == dynamic([3,4,5,-1,-1])`  
   `print array_shift_left(dynamic(['a', 'b', 'c']), 2) == dynamic(['c','',''])`  

- [array_shift_right](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_rightfunction)  
   `print array_shift_right(dynamic([1,2,3,4,5]), -2) == dynamic([3,4,5,null,null])`  
   `print array_shift_right(dynamic([1,2,3,4,5]), 2) == dynamic([null,null,1,2,3])`  
   `print array_shift_right(dynamic([1,2,3,4,5]), -2, -1) == dynamic([3,4,5,-1,-1])`  
   `print array_shift_right(dynamic(['a', 'b', 'c']), -2) == dynamic(['c','',''])`  

- [pack_array](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packarrayfunction)  
   `print x = 1, y = x * 2, z = y * 2, pack_array(x,y,z)`  

   Please note that only arrays of elements of the same type may be created at this time. The underlying reasons are explained under the release note section of the `dynamic` data type.

- [repeat](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/repeatfunction)  
   `print repeat(1, 0) == dynamic([])`   
   `print repeat(1, 3) == dynamic([1, 1, 1])`  
   `print repeat("asd", 3) == dynamic(['asd', 'asd', 'asd'])`  
   `print repeat(timespan(1d), 3) == dynamic([86400, 86400, 86400])`  
   `print repeat(true, 3) == dynamic([true, true, true])`  

- [zip](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/zipfunction)  
   `print zip(dynamic([1,3,5]), dynamic([2,4,6]))`  

   Please note that only arrays of the same type are supported in our current implementation. The underlying reasons are explained under the release note section of the `dynamic` data type.

## Data types
 - [dynamic](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic)  
   `print isnull(dynamic(null))`  
   `print dynamic(1) == 1`  
   `print dynamic(timespan(1d)) == 86400`  
   `print dynamic([1, 2, 3])`  
   `print dynamic([[1], [2], [3]])`  
   `print dynamic(['a', "b", 'c'])`  

   According to the KQL specifications `dynamic` is a literal, which means that no function calls are permitted. Expressions producing literals such as `datetime` and `timespan` and their aliases (ie. `date` and `time`, respectively) along with nested `dynamic` literals are allowed.

   Please note that our current implementation supports only scalars and arrays made up of elements of the same type. Support for mixed types and property bags is deferred for now, based on our understanding of the required effort and discussion with representatives of the QRadar team.

## Mathematical functions  
 - [isnan](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnanfunction)  
   `print isnan(double(nan)) == true`  
   `print isnan(4.2) == false`  
   `print isnan(4) == false`  
   `print isnan(real(+inf)) == false`  

## Set functions
Please note that functions returning arrays with set semantics may return them in any particular order, which may be subject to change in the future.

 - [jaccard_index](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/jaccard-index-function)  
   `print jaccard_index(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3, 4, 4, 4])) == 0.75`  
   `print jaccard_index(dynamic([1, 2, 3]), dynamic([])) == 0`  
   `print jaccard_index(dynamic([]), dynamic([1, 2, 3, 4])) == 0`  
   `print isnan(jaccard_index(dynamic([]), dynamic([])))`  
   `print jaccard_index(dynamic([1, 2, 3]), dynamic([4, 5, 6, 7])) == 0`  
   `print jaccard_index(dynamic(['a', 's', 'd']), dynamic(['f', 'd', 's', 'a'])) == 0.75`  
   `print jaccard_index(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])) == 0.25`  

 - [set_difference](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setdifferencefunction)  
   `print set_difference(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3])) == dynamic([])`  
   `print array_sort_asc(set_difference(dynamic([1, 4, 2, 3, 5, 4, 6]), dynamic([1, 2, 3])))[1] == dynamic([4, 5, 6])`  
   `print set_difference(dynamic([4]), dynamic([1, 2, 3])) == dynamic([4])`  
   `print array_sort_asc(set_difference(dynamic([1, 2, 3, 4, 5]), dynamic([5]), dynamic([2, 4])))[1] == dynamic([1, 3])`  
   `print array_sort_asc(set_difference(dynamic([1, 2, 3]), dynamic([])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_difference(dynamic(['a', 's', 'd']), dynamic(['a', 'f'])))[1] == dynamic(['d', 's'])`  
   `print array_sort_asc(set_difference(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])))[1] == dynamic(['Chewbacca', 'Han Solo'])`  

 - [set_has_element](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sethaselementfunction)  
   `print set_has_element(dynamic(["this", "is", "an", "example"]), "example") == true`  
   `print set_has_element(dynamic(["this", "is", "an", "example"]), "test") == false`  
   `print set_has_element(dynamic([1, 2, 3]), 2) == true`  
   `print set_has_element(dynamic([1, 2, 3, 4.2]), 4) == false`  

 - [set_intersect](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setintersectfunction)  
   `print array_sort_asc(set_intersect(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_intersect(dynamic([1, 4, 2, 3, 5, 4, 6]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3])`  
   `print set_intersect(dynamic([4]), dynamic([1, 2, 3])) == dynamic([])`  
   `print set_intersect(dynamic([1, 2, 3, 4, 5]), dynamic([1, 3, 5]), dynamic([2, 5])) == dynamic([5])`  
   `print set_intersect(dynamic([1, 2, 3]), dynamic([])) == dynamic([])`  
   `print set_intersect(dynamic(['a', 's', 'd']), dynamic(['a', 'f'])) == dynamic(['a'])`  
   `print set_intersect(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])) == dynamic(['Darth Vader'])`  

 - [set_union](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setunionfunction)  
   `print array_sort_asc(set_union(dynamic([1, 1, 2, 2, 3, 3]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_union(dynamic([1, 4, 2, 3, 5, 4, 6]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3, 4, 5, 6])`  
   `print array_sort_asc(set_union(dynamic([4]), dynamic([1, 2, 3])))[1] == dynamic([1, 2, 3, 4])`  
   `print array_sort_asc(set_union(dynamic([1, 3, 4]), dynamic([5]), dynamic([2, 4])))[1] == dynamic([1, 2, 3, 4, 5])`  
   `print array_sort_asc(set_union(dynamic([1, 2, 3]), dynamic([])))[1] == dynamic([1, 2, 3])`  
   `print array_sort_asc(set_union(dynamic(['a', 's', 'd']), dynamic(['a', 'f'])))[1] == dynamic(['a', 'd', 'f', 's'])`  
   `print array_sort_asc(set_union(dynamic(['Chewbacca', 'Darth Vader', 'Han Solo']), dynamic(['Darth Sidious', 'Darth Vader'])))[1] == dynamic(['Chewbacca', 'Darth Sidious', 'Darth Vader', 'Han Solo'])`  

# August 29, 2022

## **mv-expand operator**
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator
Note: **expand on array columns only**
- test cases 
   ```
   CREATE TABLE T
   (    
      a UInt8,
      b Array(String),
      c Array(Int8),
      d Array(Int8)
   ) ENGINE = Memory;

   INSERT INTO T VALUES (1, ['Salmon', 'Steak','Chicken'],[1,2,3,4],[5,6,7,8])
   
   T | mv-expand c  
   T | mv-expand c, d  
   T | mv-expand b | mv-expand c  
   T | mv-expand c to typeof(bool)  
   T | mv-expand with_itemindex=index b, c, d  
   T | mv-expand array_concat(c,d)   
   T | mv-expand x = c, y = d   
   T | mv-expand xy = array_concat(c, d)  
   T | mv-expand with_itemindex=index c,d to typeof(bool)  
   ```

## **make-series operator**  
https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-seriesoperator

- test case make-series on datetime column  
   ```
   CREATE TABLE T
   (    
      Supplier Nullable(String),
      Fruit String ,
      Price Float64,
      Purchase Date 
   ) ENGINE = Memory;

   INSERT INTO T VALUES  ('Aldi','Apple',4,'2016-09-10');
   INSERT INTO T VALUES  ('Costco','Apple',2,'2016-09-11');
   INSERT INTO T VALUES  ('Aldi','Apple',6,'2016-09-10');
   INSERT INTO T VALUES  ('Costco','Snargaluff',100,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Apple',7,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Snargaluff',400,'2016-09-11');
   INSERT INTO T VALUES  ('Costco','Snargaluff',104,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Apple',5,'2016-09-12');
   INSERT INTO T VALUES  ('Aldi','Snargaluff',600,'2016-09-11');
   INSERT INTO T VALUES  ('Costco','Snargaluff',200,'2016-09-10');
   ```  
   Have from and to  
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  to datetime(2016-09-13) step 1d by Supplier, Fruit
   ```
   Has from , without to  
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  step 1d by Supplier, Fruit
   ```
   Without from , has to  
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase  to datetime(2016-09-13) step 1d by Supplier, Fruit
   ```
   Without from , without to
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase step 1d by Supplier, Fruit
   ```
   Without by clause
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase step 1d
   ```
   Without aggregation alias
   ```
   T |  make-series avg(Price) default=0 on Purchase step 1d by Supplier, Fruit
   ```
   Has group expression alias
   ```
   T |  make-series avg(Price) default=0 on Purchase step 1d by Supplier_Name = Supplier, Fruit
   ```
   Use different step value
   ```
   T |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  to datetime(2016-09-13) step 3d by Supplier, Fruit
   ```
- test case make-series on numeric column  
   ```
   CREATE TABLE T2
   (    
      Supplier Nullable(String),
      Fruit String ,
      Price Int32,
      Purchase Int32  
   ) ENGINE = Memory;

   INSERT INTO T2 VALUES  ('Aldi','Apple',4,10);
   INSERT INTO T2 VALUES  ('Costco','Apple',2,11);
   INSERT INTO T2 VALUES  ('Aldi','Apple',6,10);
   INSERT INTO T2 VALUES  ('Costco','Snargaluff',100,12);
   INSERT INTO T2 VALUES  ('Aldi','Apple',7,12);
   INSERT INTO T2 VALUES  ('Aldi','Snargaluff',400,11);
   INSERT INTO T2 VALUES  ('Costco','Snargaluff',104,12);
   INSERT INTO T2 VALUES  ('Aldi','Apple',5,12);
   INSERT INTO T2 VALUES  ('Aldi','Snargaluff',600,11);
   INSERT INTO T2 VALUES  ('Costco','Snargaluff',200,10);
   ```
   Have from and to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase from 10 to  15 step  1.0  by Supplier, Fruit;
   ```
   Has from , without to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase from 10 step  1.0  by Supplier, Fruit;
   ```
   Without from , has to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase to 18 step  4.0  by Supplier, Fruit;
   ```
   Without from , without to  
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase step  2.0  by Supplier, Fruit;
   ```
   Without by clause
   ```
   T2 | make-series PriceAvg=avg(Price) default=0 on Purchase step  2.0;
   ```

## Aggregate Functions
- [bin](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction)  
   `print bin(4.5, 1)`  
   `print bin(time(16d), 7d)`  
   `print bin(datetime(1970-05-11 13:45:07), 1d)`  
- [stdev](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdev-aggfunction)  
   `Customers | summarize t = stdev(Age) by FirstName`  

- [stdevif](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdevif-aggfunction)  
   `Customers | summarize t = stdevif(Age, Age < 10) by FirstName`  

- [binary_all_and](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-all-and-aggfunction)  
   `Customers | summarize t = binary_all_and(Age) by FirstName`  

- [binary_all_or](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-all-or-aggfunction)  
   `Customers | summarize t = binary_all_or(Age) by FirstName`  

- [binary_all_xor](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-all-xor-aggfunction)  
   `Customers | summarize t = binary_all_xor(Age) by FirstName`  

- [percentiles](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `Customers | summarize percentiles(Age, 30, 40, 50, 60, 70) by FirstName`  

- [percentilesw](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `DataTable | summarize t = percentilesw(Bucket, Frequency, 50, 75, 99.9)`  

- [percentile](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `Customers | summarize t = percentile(Age, 50) by FirstName`  

- [percentilew](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction)  
   `DataTable | summarize t = percentilew(Bucket, Frequency, 50)`  

## Dynamic functions
- [array_sort_asc](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysortascfunction)  
   **Only support the constant dynamic array.**  
   **Returns an array. So, each element of the input has to be of same datatype.**  
   `print t = array_sort_asc(dynamic([null, 'd', 'a', 'c', 'c']))`  
   `print t = array_sort_asc(dynamic([4, 1, 3, 2]))`  
   `print t = array_sort_asc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))`  
   `print t = array_sort_asc(dynamic(['q', 'p', 'r']), dynamic(['clickhouse','hello', 'world']))`  
   `print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , false)`  
   `print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)`  
   `print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)`  
   `print t = array_sort_asc( dynamic([null, null, null]) , false)`  
   `print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 > 2)`  
   `print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30, 50, 3]), 1 > 2)`  

- [array_sort_desc](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysortdescfunction) **(only support the constant dynamic array)**  
   
   `print t = array_sort_desc(dynamic([null, 'd', 'a', 'c', 'c']))`  
   `print t = array_sort_desc(dynamic([4, 1, 3, 2]))`  
   `print t = array_sort_desc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))`  
   `print t = array_sort_desc(dynamic(['q', 'p', 'r']), dynamic(['clickhouse','hello', 'world']))`  
   `print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , false)`  
   `print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)`  
   `print t = array_sort_desc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)`  
   `print t = array_sort_desc( dynamic([null, null, null]) , false)`  
   `print t = array_sort_desc(dynamic([2, 1, null, 3]), dynamic([20, 10, 40, 30]), 1 > 2)`  
   `print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50, 3]), 1 > 2)`  

- [array_concat](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayconcatfunction)  
   `print array_concat(dynamic([1, 2, 3]), dynamic([4, 5]), dynamic([6, 7, 8, 9])) == dynamic([1, 2, 3, 4, 5, 6, 7, 8, 9])`  

- [array_iff / array_iif](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayifffunction)  
   `print array_iif(dynamic([true, false, true]), dynamic([1, 2, 3]), dynamic([4, 5, 6])) == dynamic([1, 5, 3])`  
   `print array_iif(dynamic([true, false, true]), dynamic([1, 2, 3, 4]), dynamic([4, 5, 6])) == dynamic([1, 5, 3])`  
   `print array_iif(dynamic([true, false, true, false]), dynamic([1, 2, 3, 4]), dynamic([4, 5, 6])) == dynamic([1, 5, 3, null])`  
   `print array_iif(dynamic([1, 0, -1, 44, 0]), dynamic([1, 2, 3, 4]), dynamic([4, 5, 6])) == dynamic([1, 5, 3, 4, null])`  

- [array_slice](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayslicefunction)  
   `print array_slice(dynamic([1,2,3]), 1, 2) == dynamic([2, 3])`  
   `print array_slice(dynamic([1,2,3,4,5]), 2, -1) == dynamic([3, 4, 5])`  
   `print array_slice(dynamic([1,2,3,4,5]), -3, -2) == dynamic([3, 4])`  

- [array_split](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysplitfunction)  
   `print array_split(dynamic([1,2,3,4,5]), 2) == dynamic([[1,2],[3,4,5]])`  
   `print array_split(dynamic([1,2,3,4,5]), dynamic([1,3])) == dynamic([[1],[2,3],[4,5]])`  

## DateTimeFunctions

- [ago](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/agofunction)  
   `print ago(2h)`  

- [endofday](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofdayfunction)  
   `print endofday(datetime(2017-01-01 10:10:17), -1)`  
   `print endofday(datetime(2017-01-01 10:10:17), 1)`  
   `print endofday(datetime(2017-01-01 10:10:17))`  

- [endofmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofmonthfunction)  
   `print endofmonth(datetime(2017-01-01 10:10:17), -1)`  
   `print endofmonth(datetime(2017-01-01 10:10:17), 1)`  
   `print endofmonth(datetime(2017-01-01 10:10:17))`  

- [endofweek](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofweekfunction)  
   `print endofweek(datetime(2017-01-01 10:10:17), 1)`  
   `print endofweek(datetime(2017-01-01 10:10:17), -1)`  
   `print endofweek(datetime(2017-01-01 10:10:17))`  

- [endofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofyearfunction)  
   `print endofyear(datetime(2017-01-01 10:10:17), -1)`  
   `print endofyear(datetime(2017-01-01 10:10:17), 1)`  
   `print endofyear(datetime(2017-01-01 10:10:17))`  

- [make_datetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-datetimefunction)  
   `print make_datetime(2017,10,01)`  
   `print make_datetime(2017,10,01,12,10)`  
   `print make_datetime(2017,10,01,12,11,0.1234567)`  

-  [datetime_diff](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-difffunction)  
   `print datetime_diff('year',datetime(2017-01-01),datetime(2000-12-31))`  
   `print datetime_diff('quarter',datetime(2017-07-01),datetime(2017-03-30))`  
   `print datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59))`  

- [unixtime_microseconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-microseconds-todatetimefunction)  
   `print unixtime_microseconds_todatetime(1546300800000000)`  

- [unixtime_milliseconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-milliseconds-todatetimefunction)  
   `print unixtime_milliseconds_todatetime(1546300800000)`  

- [unixtime_nanoseconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-nanoseconds-todatetimefunction)  
   `print unixtime_nanoseconds_todatetime(1546300800000000000)`  

- [datetime_part](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-partfunction)  
   `print datetime_part('day', datetime(2017-10-30 01:02:03.7654321))`  

- [datetime_add](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-addfunction)  
   `print datetime_add('day',1,datetime(2017-10-30 01:02:03.7654321))`  

- [format_timespan](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-timespanfunction)  
   `print format_timespan(time(1d), 'd-[hh:mm:ss]')`  
   `print format_timespan(time('12:30:55.123'), 'ddddd-[hh:mm:ss.ffff]')`  

- [format_datetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-datetimefunction)  
   `print format_datetime(todatetime('2009-06-15T13:45:30.6175425'), 'yy-M-dd [H:mm:ss.fff]')`  
   `print format_datetime(datetime(2015-12-14 02:03:04.12345), 'y-M-d h:m:s tt')`  

- [todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todatetimefunction)  
   `print  todatetime('2014-05-25T08:20:03.123456Z')`  
   `print  todatetime('2014-05-25 20:03.123')`  

- [totimespan] (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/totimespanfunction)
   ` print totimespan('0.01:34:23')`
   `print totimespan(1d)`

# August 15, 2022
   **double quote support**  
   ``print res = strcat("double ","quote")``  
## Aggregate functions
 - [bin_at](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction)  
   `print res = bin_at(6.5, 2.5, 7)`  
   `print res = bin_at(1h, 1d, 12h)`  
   `print res = bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0))`  
   `print res = bin_at(datetime(2017-05-17 10:20:00.0), 7d, datetime(2017-06-04 00:00:00.0))`  

 - [array_index_of](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayindexoffunction)  
    *Supports only basic lookup. Do not support start_index, length and occurrence*  
    `print output = array_index_of(dynamic(['John', 'Denver', 'Bob', 'Marley']), 'Marley')`  
    `print output = array_index_of(dynamic([1, 2, 3]), 2)`  
 - [array_sum](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array-sum-function)  
    `print output = array_sum(dynamic([2, 5, 3]))`  
    `print output = array_sum(dynamic([2.5, 5.5, 3]))`  
 - [array_length](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraylengthfunction)  
    `print output = array_length(dynamic(['John', 'Denver', 'Bob', 'Marley']))`  
    `print output = array_length(dynamic([1, 2, 3]))`

## Conversion
- [tobool / toboolean](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toboolfunction)
   `print tobool(true) == true`
   `print toboolean(false) == false`
   `print tobool(0) == false`
   `print toboolean(19819823) == true`
   `print tobool(-2) == true`
   `print isnull(toboolean('a'))`
   `print tobool('true') == true`
   `print toboolean('false') == false`

- [todouble / toreal](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todoublefunction)
   `print todouble(4) == 4`
   `print toreal(4.2) == 4.2`
   `print isnull(todouble('a'))`
   `print toreal('-0.3') == -0.3`

- [toint](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tointfunction)
   `print isnull(toint('a'))`  
   `print toint(4) == 4`  
   `print toint('4') == 4`  
   `print isnull(toint(4.2))`  

- [tostring](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tostringfunction)
   `print tostring(123) == '123'`  
   `print tostring('asd') == 'asd'`  

## Data Types
 - [dynamic](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic)  
    *Supports only 1D array*  
    `print output = dynamic(['a', 'b', 'c'])`  
    `print output = dynamic([1, 2, 3])`  
 
- [bool,boolean](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/bool)  
   `print bool(1)`  
   `print boolean(0)`  

- [datetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/datetime)  
   `print datetime(2015-12-31 23:59:59.9)`  
   `print datetime('2015-12-31 23:59:59.9')`  
   `print datetime("2015-12-31:)`  

- [guid](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/guid)  
   `print guid(74be27de-1e4e-49d9-b579-fe0b331d3642)`  
   `print guid('74be27de-1e4e-49d9-b579-fe0b331d3642')`  
   `print guid('74be27de1e4e49d9b579fe0b331d3642')`  

- [int](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/int)  
   `print int(1)`  

- [long](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/long)  
   `print long(16)`  

- [real](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/real)  
   `print real(1)`  

- [timespan ,time](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/timespan)  
   **Note** the timespan is used for calculating datatime, so the output is in seconds. e.g. time(1h) = 3600
   `print 1d`  
   `print 30m`  
   `print time('0.12:34:56.7')`  
   `print time(2h)`  
   `print timespan(2h)`  


## StringFunctions
 
- [base64_encode_fromguid](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/base64-encode-fromguid-function)  
`print Quine = base64_encode_fromguid('ae3133f2-6e22-49ae-b06a-16e6a9b212eb')`  
- [base64_decode_toarray](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/base64_decode_toarrayfunction)  
`print base64_decode_toarray('S3VzdG8=')`  
- [base64_decode_toguid](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/base64-decode-toguid-function)  
`print base64_decode_toguid('YWUzMTMzZjItNmUyMi00OWFlLWIwNmEtMTZlNmE5YjIxMmVi')`  
- [replace_regex](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/replace-regex-function)  
`print replace_regex('Hello, World!', '.', '\\0\\0')`  
- [has_any_index](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/has-any-index-function)  
`print idx = has_any_index('this is an example', dynamic(['this', 'example']))`
- [translate](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/translatefunction)  
`print translate('krasp', 'otsku', 'spark')`  
- [trim](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimfunction)  
`print trim('--', '--https://bing.com--')`  
- [trim_end](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimendfunction)  
`print trim_end('.com', 'bing.com')`  
- [trim_start](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimstartfunction)  
`print trim_start('[^\\w]+', strcat('-  ','Te st1','// $'))`  

## DateTimeFunctions
- [startofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofyearfunction)  
   `print startofyear(datetime(2017-01-01 10:10:17), -1)`  
   `print startofyear(datetime(2017-01-01 10:10:17), 0)`  
   `print startofyear(datetime(2017-01-01 10:10:17), 1)`  
- [weekofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/weekofyearfunction)  
   `print week_of_year(datetime(2020-12-31))`  
   `print week_of_year(datetime(2020-06-15))`  
   `print week_of_year(datetime(1970-01-01))`  
   `print  week_of_year(datetime(2000-01-01))`  

- [startofweek](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofweekfunction)  
   `print startofweek(datetime(2017-01-01 10:10:17), -1)`  
   `print startofweek(datetime(2017-01-01 10:10:17), 0)`  
   `print startofweek(datetime(2017-01-01 10:10:17), 1)`  

- [startofmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofmonthfunction)  
   `print startofmonth(datetime(2017-01-01 10:10:17), -1)`  
   `print startofmonth(datetime(2017-01-01 10:10:17), 0)`  
   `print startofmonth(datetime(2017-01-01 10:10:17), 1)`  

- [startofday](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofdayfunction)  
   `print startofday(datetime(2017-01-01 10:10:17), -1)`  
   `print startofday(datetime(2017-01-01 10:10:17), 0)`  
   `print startofday(datetime(2017-01-01 10:10:17), 1)`  

- [monthofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/monthofyearfunction)  
   `print monthofyear(datetime("2015-12-14"))`  

- [hourofday](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hourofdayfunction)  
   `print hourofday(datetime(2015-12-14 18:54:00))`  

- [getyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getyearfunction)  
   `print getyear(datetime(2015-10-12))`  

- [getmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getmonthfunction)  
   `print getmonth(datetime(2015-10-12))`  

- [dayofyear](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dayofyearfunction)  
   `print dayofyear(datetime(2015-12-14))`  

- [dayofmonth](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dayofmonthfunction)  
   `print (datetime(2015-12-14))`  

- [unixtime_seconds_todatetime](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/unixtime-seconds-todatetimefunction)  
   `print unixtime_seconds_todatetime(1546300800)`  

- [dayofweek](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dayofweekfunction)  
   `print dayofweek(datetime(2015-12-20))`  

- [now](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/nowfunction)  
   `print now()`  
   `print now(2d)`  
   `print now(-2h)`  
   `print now(5microseconds)`  
   `print now(5seconds)`  
   `print now(6minutes)`  
   `print now(-2d) `  
   `print now(time(1d))`  


## Binary functions
- [binary_and](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-andfunction)  
   `print binary_and(15, 3) == 3`  
   `print binary_and(1, 2) == 0`  
- [binary_not](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-notfunction)  
   `print binary_not(1) == -2`  
- [binary_or](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-orfunction)  
   `print binary_or(3, 8) == 11`  
   `print binary_or(1, 2) == 3`  
- [binary_shift_left](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-shift-leftfunction)  
   `print binary_shift_left(1, 1) == 2`  
   `print binary_shift_left(1, 64) == 1`  
- [binary_shift_right](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-shift-rightfunction)  
   `print binary_shift_right(1, 1) == 0`
   `print binary_shift_right(1, 64) == 1`
- [binary_xor](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binary-xorfunction)  
   `print binary_xor(1, 3) == 2`  
- [bitset_count_ones](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bitset-count-onesfunction)  
   `print bitset_count_ones(42) == 3`  

## IP functions
- [format_ipv4](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-ipv4-function)  
   `print format_ipv4('192.168.1.255', 24) == '192.168.1.0'`  
   `print format_ipv4(3232236031, 24) == '192.168.1.0'`  
- [format_ipv4_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-ipv4-mask-function)  
   `print format_ipv4_mask('192.168.1.255', 24) == '192.168.1.0/24'`  
   `print format_ipv4_mask(3232236031, 24) == '192.168.1.0/24'`  
- [ipv4_compare](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-comparefunction)  
   `print ipv4_compare('127.0.0.1', '127.0.0.1') == 0`  
   `print ipv4_compare('192.168.1.1', '192.168.1.255') < 0`  
   `print ipv4_compare('192.168.1.1/24', '192.168.1.255/24') == 0`  
   `print ipv4_compare('192.168.1.1', '192.168.1.255', 24) == 0`  
- [ipv4_is_match](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-matchfunction)  
   `print ipv4_is_match('127.0.0.1', '127.0.0.1') == true`  
   `print ipv4_is_match('192.168.1.1', '192.168.1.255') == false`  
   `print ipv4_is_match('192.168.1.1/24', '192.168.1.255/24') == true`  
   `print ipv4_is_match('192.168.1.1', '192.168.1.255', 24) == true`  
- [ipv6_compare](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv6-comparefunction)  
   `print ipv6_compare('::ffff:7f00:1', '127.0.0.1') == 0`  
   `print ipv6_compare('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995') < 0`  
   `print ipv6_compare('192.168.1.1/24', '192.168.1.255/24') == 0`  
   `print ipv6_compare('fe80::85d:e82c:9446:7994/127', 'fe80::85d:e82c:9446:7995/127') == 0`  
   `print ipv6_compare('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995', 127) == 0`  
- [ipv6_is_match](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv6-is-matchfunction)  
   `print ipv6_is_match('::ffff:7f00:1', '127.0.0.1') == true`  
   `print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995') == false`  
   `print ipv6_is_match('192.168.1.1/24', '192.168.1.255/24') == true`  
   `print ipv6_is_match('fe80::85d:e82c:9446:7994/127', 'fe80::85d:e82c:9446:7995/127') == true`  
   `print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995', 127) == true`  
- [parse_ipv4_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv4-maskfunction)  
   `print parse_ipv4_mask('127.0.0.1', 24) == 2130706432`  
   `print parse_ipv4_mask('192.1.168.2', 31) == 3221334018`  
   `print parse_ipv4_mask('192.1.168.3', 31) == 3221334018`  
   `print parse_ipv4_mask('127.2.3.4', 32) == 2130838276`  
- [parse_ipv6_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv6-maskfunction)  
   `print parse_ipv6_mask('127.0.0.1', 24) == '0000:0000:0000:0000:0000:ffff:7f00:0000'`  
   `print parse_ipv6_mask('fe80::85d:e82c:9446:7994', 120) == 'fe80:0000:0000:0000:085d:e82c:9446:7900'`  

# August 1, 2022

**The config setting to allow modify dialect setting**.
   - Set dialect setting in  server configuration XML at user level(` users.xml `). This sets the ` dialect ` at server startup and CH will do query parsing for all users with ` default ` profile according to dialect value.

   For example:
   ` <profiles>
        <!-- Default settings. -->
        <default>
            <load_balancing>random</load_balancing>
            <dialect>kusto</dialect>
        </default> `
   
   - Query can be executed with HTTP client as below once dialect is set in users.xml
      ` echo "KQL query" | curl -sS "http://localhost:8123/?" --data-binary @- `
   
   - To execute the query using clickhouse-client , Update clickhouse-client.xml as below and connect clickhouse-client with --config-file option (` clickhouse-client --config-file=<config-file path> `) 

     ` <config>
         <dialect>kusto</dialect>
      </config>  `

   OR 
      pass dialect setting with '--'. For example : 
      ` clickhouse-client --dialect='kusto' -q "KQL query" `

- **strcmp** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcmpfunction)  
   `print strcmp('abc','ABC')`

- **parse_url** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parseurlfunction)  
   `print Result = parse_url('scheme://username:password@www.google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')`

- **parse_urlquery** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parseurlqueryfunction)  
   `print Result = parse_urlquery('k1=v1&k2=v2&k3=v3')`

- **print operator** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/printoperator)  
   `print x=1, s=strcat('Hello', ', ', 'World!')`

- **Aggregate Functions:**
 - [make_list()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makelist-aggfunction)  
   `Customers | summarize t = make_list(FirstName) by FirstName`
   `Customers | summarize t = make_list(FirstName, 10) by FirstName`
 - [make_list_if()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makelistif-aggfunction)  
   `Customers | summarize t = make_list_if(FirstName, Age > 10) by FirstName`
   `Customers | summarize t = make_list_if(FirstName, Age > 10, 10) by FirstName`
 - [make_list_with_nulls()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-list-with-nulls-aggfunction)  
   `Customers | summarize t = make_list_with_nulls(Age) by FirstName`
 - [make_set()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makeset-aggfunction)  
   `Customers | summarize t = make_set(FirstName) by FirstName`
   `Customers | summarize t = make_set(FirstName, 10) by FirstName`
 - [make_set_if()](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makesetif-aggfunction)  
   `Customers | summarize t = make_set_if(FirstName, Age > 10) by FirstName`
   `Customers | summarize t = make_set_if(FirstName, Age > 10, 10) by FirstName`

## IP functions

- **The following functions now support arbitrary expressions as their argument:**
   - [ipv4_is_private](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-privatefunction)
   - [ipv4_is_in_range](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-in-range-function)
   - [ipv4_netmask_suffix](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-netmask-suffix-function)
      
# July 17, 2022

## Renamed dialect from sql_dialect to dialect

`set dialect='clickhouse'`  
`set dialect='kusto'`  

## IP functions
- [parse_ipv4](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv4function)
   `"Customers | project parse_ipv4('127.0.0.1')"`
- [parse_ipv6](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parse-ipv6function)
   `"Customers | project parse_ipv6('127.0.0.1')"`

Please note that the functions listed below only take constant parameters for now. Further improvement is to be expected to support expressions.

- [ipv4_is_private](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-privatefunction)
   `"Customers | project ipv4_is_private('192.168.1.6/24')"`
   `"Customers | project ipv4_is_private('192.168.1.6')"`
- [ipv4_is_in_range](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-is-in-range-function)
   `"Customers | project ipv4_is_in_range('127.0.0.1', '127.0.0.1')"`
   `"Customers | project ipv4_is_in_range('192.168.1.6', '192.168.1.1/24')"`
- [ipv4_netmask_suffix](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv4-netmask-suffix-function)
   `"Customers | project ipv4_netmask_suffix('192.168.1.1/24')"`
   `"Customers | project ipv4_netmask_suffix('192.168.1.1')"`

## string functions
- **support subquery for `in` orerator** (https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/in-cs-operator)  
 (subquery need to be wrapped with bracket inside bracket)

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

## KQL() function

 - create table  
  `CREATE TABLE kql_table4 ENGINE = Memory AS select *, now() as new_column From kql($$Customers | project LastName,Age$$);`  
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
    `INSERT INTO temp select * from kql($$Customers|project FirstName,LastName,Age$$);`  
    verify the content of `temp`   
        `select * from temp`

 - Select from kql(...)  
    `Select * from kql($$Customers|project FirstName$$)`

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
