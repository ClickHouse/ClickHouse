---
toc_priority: 4
toc_title: Hive
---

# Hive {#hive}

Hive引擎允许对HDFS Hive表执行 `SELECT` 查询。目前它支持如下输入格式:

-文本:只支持简单的标量列类型，除了 `Binary`

- ORC:支持简单的标量列类型，除了`char`; 只支持 `array` 这样的复杂类型

- Parquet:支持所有简单标量列类型;只支持 `array` 这样的复杂类型

## 创建表 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Hive('thrift://host:port', 'database', 'table');
PARTITION BY expr
```
查看[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query)查询的详细描述。

表的结构可以与原来的Hive表结构有所不同:
- 列名应该与原来的Hive表相同，但你可以使用这些列中的一些，并以任何顺序，你也可以使用一些从其他列计算的别名列。
- 列类型与原Hive表的列类型保持一致。
- “Partition by expression”应与原Hive表保持一致，“Partition by expression”中的列应在表结构中。

**引擎参数**

-   `thrift://host:port` — Hive Metastore 地址

-   `database` — 远程数据库名.

-   `table` — 远程数据表名.

## 使用示例 {#usage-example}

### 如何使用HDFS文件系统的本地缓存
我们强烈建议您为远程文件系统启用本地缓存。基准测试显示，如果使用缓存，它的速度会快两倍。

在使用缓存之前，请将其添加到 `config.xml`
``` xml
<local_cache_for_remote_fs>
    <enable>true</enable>
    <root_dir>local_cache</root_dir>
    <limit_size>559096952</limit_size>
    <bytes_read_before_flush>1048576</bytes_read_before_flush>
</local_cache_for_remote_fs>
```


- enable: 开启后，ClickHouse将为HDFS (远程文件系统)维护本地缓存。
- root_dir: 必需的。用于存储远程文件系统的本地缓存文件的根目录。
- limit_size: 必需的。本地缓存文件的最大大小(单位为字节)。
- bytes_read_before_flush: 从远程文件系统下载文件时，刷新到本地文件系统前的控制字节数。缺省值为1MB。

当ClickHouse为远程文件系统启用了本地缓存时，用户仍然可以选择不使用缓存，并在查询中设置`use_local_cache_for_remote_fs = 0 `, `use_local_cache_for_remote_fs` 默认为 `false`。

### 查询 ORC 输入格式的Hive 表

#### 在 Hive 中建表
``` text
hive > CREATE TABLE `test`.`test_orc`(
  `f_tinyint` tinyint, 
  `f_smallint` smallint, 
  `f_int` int, 
  `f_integer` int, 
  `f_bigint` bigint, 
  `f_float` float, 
  `f_double` double, 
  `f_decimal` decimal(10,0), 
  `f_timestamp` timestamp, 
  `f_date` date, 
  `f_string` string, 
  `f_varchar` varchar(100), 
  `f_bool` boolean, 
  `f_binary` binary, 
  `f_array_int` array<int>, 
  `f_array_string` array<string>, 
  `f_array_float` array<float>, 
  `f_array_array_int` array<array<int>>, 
  `f_array_array_string` array<array<string>>, 
  `f_array_array_float` array<array<float>>)
PARTITIONED BY ( 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://testcluster/data/hive/test.db/test_orc'

OK
Time taken: 0.51 seconds

hive > insert into test.test_orc partition(day='2021-09-18') select 1, 2, 3, 4, 5, 6.11, 7.22, 8.333, current_timestamp(), current_date(), 'hello world', 'hello world', 'hello world', true, 'hello world', array(1, 2, 3), array('hello world', 'hello world'), array(float(1.1), float(1.2)), array(array(1, 2), array(3, 4)), array(array('a', 'b'), array('c', 'd')), array(array(float(1.11), float(2.22)), array(float(3.33), float(4.44)));
OK
Time taken: 36.025 seconds

hive > select * from test.test_orc;
OK
1	2	3	4	5	6.11	7.22	8	2021-11-05 12:38:16.314	2021-11-05	hello world	hello world	hello world                                                                                         	true	hello world	[1,2,3]	["hello world","hello world"]	[1.1,1.2]	[[1,2],[3,4]]	[["a","b"],["c","d"]]	[[1.11,2.22],[3.33,4.44]]	2021-09-18
Time taken: 0.295 seconds, Fetched: 1 row(s)
```

#### 在 ClickHouse 中建表

ClickHouse中的表，从上面创建的Hive表中获取数据:

``` sql
CREATE TABLE test.test_orc
(
    `f_tinyint` Int8,
    `f_smallint` Int16,
    `f_int` Int32,
    `f_integer` Int32,
    `f_bigint` Int64,
    `f_float` Float32,
    `f_double` Float64,
    `f_decimal` Float64,
    `f_timestamp` DateTime,
    `f_date` Date,
    `f_string` String,
    `f_varchar` String,
    `f_bool` Bool,
    `f_binary` String,
    `f_array_int` Array(Int32),
    `f_array_string` Array(String),
    `f_array_float` Array(Float32),
    `f_array_array_int` Array(Array(Int32)),
    `f_array_array_string` Array(Array(String)),
    `f_array_array_float` Array(Array(Float32)),
    `day` String
)
ENGINE = Hive('thrift://202.168.117.26:9083', 'test', 'test_orc')
PARTITION BY day

```

``` sql
SELECT * FROM test.test_orc settings input_format_orc_allow_missing_columns = 1\G
```

``` text
SELECT *
FROM test.test_orc
SETTINGS input_format_orc_allow_missing_columns = 1

Query id: c3eaffdc-78ab-43cd-96a4-4acc5b480658

Row 1:
──────
f_tinyint:            1
f_smallint:           2
f_int:                3
f_integer:            4
f_bigint:             5
f_float:              6.11
f_double:             7.22
f_decimal:            8
f_timestamp:          2021-12-04 04:00:44
f_date:               2021-12-03
f_string:             hello world
f_varchar:            hello world
f_bool:               true
f_binary:             hello world
f_array_int:          [1,2,3]
f_array_string:       ['hello world','hello world']
f_array_float:        [1.1,1.2]
f_array_array_int:    [[1,2],[3,4]]
f_array_array_string: [['a','b'],['c','d']]
f_array_array_float:  [[1.11,2.22],[3.33,4.44]]
day:                  2021-09-18


1 rows in set. Elapsed: 0.078 sec. 
```

### 查询 Parquest 输入格式的Hive 表

#### 在 Hive 中建表
``` text
hive >
CREATE TABLE `test`.`test_parquet`(
  `f_tinyint` tinyint, 
  `f_smallint` smallint, 
  `f_int` int, 
  `f_integer` int, 
  `f_bigint` bigint, 
  `f_float` float, 
  `f_double` double, 
  `f_decimal` decimal(10,0), 
  `f_timestamp` timestamp, 
  `f_date` date, 
  `f_string` string, 
  `f_varchar` varchar(100), 
  `f_char` char(100), 
  `f_bool` boolean, 
  `f_binary` binary, 
  `f_array_int` array<int>, 
  `f_array_string` array<string>, 
  `f_array_float` array<float>, 
  `f_array_array_int` array<array<int>>, 
  `f_array_array_string` array<array<string>>, 
  `f_array_array_float` array<array<float>>)
PARTITIONED BY ( 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://testcluster/data/hive/test.db/test_parquet'
OK
Time taken: 0.51 seconds

hive >  insert into test.test_parquet partition(day='2021-09-18') select 1, 2, 3, 4, 5, 6.11, 7.22, 8.333, current_timestamp(), current_date(), 'hello world', 'hello world', 'hello world', true, 'hello world', array(1, 2, 3), array('hello world', 'hello world'), array(float(1.1), float(1.2)), array(array(1, 2), array(3, 4)), array(array('a', 'b'), array('c', 'd')), array(array(float(1.11), float(2.22)), array(float(3.33), float(4.44)));
OK
Time taken: 36.025 seconds

hive > select * from test.test_parquet;
OK
1	2	3	4	5	6.11	7.22	8	2021-12-14 17:54:56.743	2021-12-14	hello world	hello world	hello world                                                                                         	true	hello world	[1,2,3]	["hello world","hello world"]	[1.1,1.2]	[[1,2],[3,4]]	[["a","b"],["c","d"]]	[[1.11,2.22],[3.33,4.44]]	2021-09-18
Time taken: 0.766 seconds, Fetched: 1 row(s)
```

####  在 ClickHouse 中建表

ClickHouse 中的表， 从上面创建的Hive表中获取数据:

``` sql
CREATE TABLE test.test_parquet
(
    `f_tinyint` Int8,
    `f_smallint` Int16,
    `f_int` Int32,
    `f_integer` Int32,
    `f_bigint` Int64,
    `f_float` Float32,
    `f_double` Float64,
    `f_decimal` Float64,
    `f_timestamp` DateTime,
    `f_date` Date,
    `f_string` String,
    `f_varchar` String,
    `f_char` String,
    `f_bool` Bool,
    `f_binary` String,
    `f_array_int` Array(Int32),
    `f_array_string` Array(String),
    `f_array_float` Array(Float32),
    `f_array_array_int` Array(Array(Int32)),
    `f_array_array_string` Array(Array(String)),
    `f_array_array_float` Array(Array(Float32)),
    `day` String
)
ENGINE = Hive('thrift://localhost:9083', 'test', 'test_parquet')
PARTITION BY day
```

``` sql
SELECT * FROM test.test_parquet settings input_format_parquet_allow_missing_columns = 1\G
```

``` text
SELECT *
FROM test_parquet
SETTINGS input_format_parquet_allow_missing_columns = 1

Query id: 4e35cf02-c7b2-430d-9b81-16f438e5fca9

Row 1:
──────
f_tinyint:            1
f_smallint:           2
f_int:                3
f_integer:            4
f_bigint:             5
f_float:              6.11
f_double:             7.22
f_decimal:            8
f_timestamp:          2021-12-14 17:54:56
f_date:               2021-12-14
f_string:             hello world
f_varchar:            hello world
f_char:               hello world
f_bool:               true
f_binary:             hello world
f_array_int:          [1,2,3]
f_array_string:       ['hello world','hello world']
f_array_float:        [1.1,1.2]
f_array_array_int:    [[1,2],[3,4]]
f_array_array_string: [['a','b'],['c','d']]
f_array_array_float:  [[1.11,2.22],[3.33,4.44]]
day:                  2021-09-18

1 rows in set. Elapsed: 0.357 sec. 
```

###  查询文本输入格式的Hive表

#### 在Hive 中建表 

``` text
hive >
CREATE TABLE `test`.`test_text`(
  `f_tinyint` tinyint, 
  `f_smallint` smallint, 
  `f_int` int, 
  `f_integer` int, 
  `f_bigint` bigint, 
  `f_float` float, 
  `f_double` double, 
  `f_decimal` decimal(10,0), 
  `f_timestamp` timestamp, 
  `f_date` date, 
  `f_string` string, 
  `f_varchar` varchar(100), 
  `f_char` char(100), 
  `f_bool` boolean, 
  `f_binary` binary, 
  `f_array_int` array<int>, 
  `f_array_string` array<string>, 
  `f_array_float` array<float>, 
  `f_array_array_int` array<array<int>>, 
  `f_array_array_string` array<array<string>>, 
  `f_array_array_float` array<array<float>>)
PARTITIONED BY ( 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://testcluster/data/hive/test.db/test_text'
Time taken: 0.1 seconds, Fetched: 34 row(s)


hive >  insert into test.test_text partition(day='2021-09-18') select 1, 2, 3, 4, 5, 6.11, 7.22, 8.333, current_timestamp(), current_date(), 'hello world', 'hello world', 'hello world', true, 'hello world', array(1, 2, 3), array('hello world', 'hello world'), array(float(1.1), float(1.2)), array(array(1, 2), array(3, 4)), array(array('a', 'b'), array('c', 'd')), array(array(float(1.11), float(2.22)), array(float(3.33), float(4.44)));
OK
Time taken: 36.025 seconds

hive > select * from test.test_text;
OK
1	2	3	4	5	6.11	7.22	8	2021-12-14 18:11:17.239	2021-12-14	hello world	hello world	hello world                                                                                         	true	hello world	[1,2,3]	["hello world","hello world"]	[1.1,1.2]	[[1,2],[3,4]]	[["a","b"],["c","d"]]	[[1.11,2.22],[3.33,4.44]]	2021-09-18
Time taken: 0.624 seconds, Fetched: 1 row(s)
```

#### 在 ClickHouse 中建表


ClickHouse中的表， 从上面创建的Hive表中获取数据:
``` sql
CREATE TABLE test.test_text
(
    `f_tinyint` Int8,
    `f_smallint` Int16,
    `f_int` Int32,
    `f_integer` Int32,
    `f_bigint` Int64,
    `f_float` Float32,
    `f_double` Float64,
    `f_decimal` Float64,
    `f_timestamp` DateTime,
    `f_date` Date,
    `f_string` String,
    `f_varchar` String,
    `f_char` String,
    `f_bool` Bool,
    `day` String
)
ENGINE = Hive('thrift://localhost:9083', 'test', 'test_text')
PARTITION BY day 
```

``` sql
SELECT * FROM test.test_text settings input_format_skip_unknown_fields = 1, input_format_with_names_use_header = 1, date_time_input_format = 'best_effort'\G
```

``` text
SELECT *
FROM test.test_text
SETTINGS input_format_skip_unknown_fields = 1, input_format_with_names_use_header = 1, date_time_input_format = 'best_effort'

Query id: 55b79d35-56de-45b9-8be6-57282fbf1f44

Row 1:
──────
f_tinyint:   1
f_smallint:  2
f_int:       3
f_integer:   4
f_bigint:    5
f_float:     6.11
f_double:    7.22
f_decimal:   8
f_timestamp: 2021-12-14 18:11:17
f_date:      2021-12-14
f_string:    hello world
f_varchar:   hello world
f_char:      hello world
f_bool:      true
day:         2021-09-18
```
