
## KQL implemented features

# August 15, 2022
## Aggregate functions
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
## DateType
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
- [parse_ipv6_mask](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ipv6-comparefunction)
   `print parse_ipv6_mask('127.0.0.1', 24) == '0000:0000:0000:0000:0000:ffff:7f00:0000'`  
   `print parse_ipv6_mask('fe80::85d:e82c:9446:7994', 120) == 'fe80:0000:0000:0000:085d:e82c:9446:7900'`  

# August 1, 2022

**The config setting to allow modify dialect setting**.
   - Set dialect setting in  server configuration XML at user level(` users.xml `). This sets the ` dialect ` at server startup and CH will do query parsing for all users with ` default ` profile acording to dialect value.

   For example:
   ` <profiles>
        <!-- Default settings. -->
        <default>
            <load_balancing>random</load_balancing>
            <dialect>kusto_auto</dialect>
        </default> `
   
   - Query can be executed with HTTP client as below once dialect is set in users.xml
      ` echo "KQL query" | curl -sS "http://localhost:8123/?" --data-binary @- `
   
   - To execute the query using clickhouse-client , Update clickhouse-client.xml as below and connect clickhouse-client with --config-file option (` clickhouse-client --config-file=<config-file path> `) 

     ` <config>
         <dialect>kusto_auto</dialect>
      </config>  `

   OR 
      pass dialect setting with '--'. For example : 
      ` clickhouse-client --dialect='kusto_auto' -q "KQL query" `

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
   `Customers | summarize t = make_list_with_nulls(FirstName) by FirstName`
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
`set dialect='kusto_auto'`

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
 