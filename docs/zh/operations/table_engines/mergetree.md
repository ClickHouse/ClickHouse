# MergeTree {#table_engines-mergetree}

Clickhouse 中最强大的表引擎当属 `MergeTree` （合并树）引擎及该家族（`*MergeTree`）中的其他引擎。

`MergeTree` 引擎家族的基本理念如下。当你有巨量数据要插入到表中，你要高效地一批批写入数据分片，并希望这些数据分片在后台按照一定规则合并。相比在插入时不断修改（重写）数据进存储，这种策略会高效很多。

主要特点:

- 存储的数据按主键排序。

    这让你可以创建一个用于快速检索数据的小稀疏索引。

- 允许使用分区，如果指定了 [主键](custom_partitioning_key.md) 的话。

    在相同数据集和相同结果集的情况下 ClickHouse 中某些带分区的操作会比普通操作更快。查询中指定了分区键时 ClickHouse 会自动截取分区数据。这也有效增加了查询性能。

- 支持数据副本。

    `ReplicatedMergeTree` 家族的表便是用于此特性。更多信息，请参阅 [数据副本](replication.md) 一节。

- 支持数据采样。

    需要的话，你可以给表设置一个采样方法。

!!! 注意
    [Merge](merge.md) 引擎并不属于 `*MergeTree` 家族。


## 建表  {#table_engine-mergetree-creating-a-table}

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

请求参数的描述，参考 [请求描述](../../query_language/create.md) 。

**子句**

- `ENGINE` - 引擎名和参数。 `ENGINE = MergeTree()`. `MergeTree` 引擎没有参数。

- `PARTITION BY` — 表的 [分区键](custom_partitioning_key.md) 。

    要按月分区，可以使用 `toYYYYMM(date_column)` 表达式，这里的 `date_column` 是一个 [Date](../../data_types/date.md) 类型的列。这里这个分区会命名为 `"YYYYMM"` 这样的格式。

- `ORDER BY` — 表的排序键.

    可以是一些列的元组或者任意的表达式。 例如: `ORDER BY (CounterID, EventDate)` 。

- `PRIMARY KEY` - 主键，如果要设置成 [跟排序键不相同](mergetree.md).

    默认情况下主键跟排序键（由 `ORDER BY` 子句指定）相同。
    因此，大部分情况下不需要再专门指定一个 `PRIMARY KEY` 子句。

- `SAMPLE BY` — 用于抽样的表达式。

    如果要用抽样表达式，主键中必须包含这个表达式。例如：
    `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

- `SETTINGS` — 用于调控 `MergeTree` 性能的额外参数：
    - `index_granularity` — 索引粒度。即索引的相邻『标记』间的数据行数。默认值，8192 。该列表中所有可用的参数可以从这里查看 [MergeTreeSettings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Storages/MergeTree/MergeTreeSettings.h) 。
    - `use_minimalistic_part_header_in_zookeeper` — 数据分片头在 ZooKeeper 中的存储方式。如果设置了 `use_minimalistic_part_header_in_zookeeper=1` ，ZooKeeper会存储更少的数据。更多信息参考『服务配置参数』这章中的 [设置描述](../server_settings/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) 。
    - `min_merge_bytes_to_use_direct_io` — 使用直接 I/O 来操作磁盘的合并操作时要求的最小数据量。合并数据分片期间，ClickHouse计算要被合并的所有数据的总存储空间。如果这个大小超过了 `min_merge_bytes_to_use_direct_io` 设置的字节数，则 ClickHouse 会使用直接 I/O 接口（`O_DIRECT` 选项）向磁盘读写。如果设置 `min_merge_bytes_to_use_direct_io = 0` ，则禁用直接 I/O。默认值：`10 * 1024 * 1024 * 1024` 字节。

**示例配置**

```
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

示例中，我们设成按月分区。

同时我们设置了一个按用户ID哈希的抽样表达式。这让你可以得到该表中每个 `CounterID` and `EventDate` 下面数据的伪随机分布。如果你在查询数据时定义了一个 [SAMPLE](../../query_language/select.md#select-sample-clause) 子句。 ClickHouse会返回相对用户数据子集来说的一个均匀的伪随机数据采样。

`index_granularity` 可省略，因为其默认值为 8192 。

<details markdown="1"><summary>已弃用的创建表方式</summary>

!!! 注意
    不要在新版项目中使用这个方式，一定要用的话，选用旧版项目。该方法描述如下。

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree() 参数**

- `date-column` — 类型为 [Date](../../data_types/date.md) 的列名。ClickHouse 会自动依据这个列按月创建分区。分区名格式为 `"YYYYMM"` 。
- `sampling_expression` — 采样表达式。
- `(primary, key)` — 主键。类型 — [Tuple()](../../data_types/tuple.md)
- `index_granularity` — 索引粒度。即索引的相邻『标记』间的数据行数。设为 8192 可以适用大部分场景。


**示例**

```
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

对于主要的配置方法，这里 `MergeTree` 引擎跟前面的例子一样，可以以同样的方式配置。
</details>

## 数据存储

表由按主键排序的数据 *分片* 组成。

当数据被插入到表中时，会分成数据片并依据主键按字典序排序。例如，如果主键是 `(CounterID, Date)` ，分片中的数据按 `CounterID` 排序，具有相同 `CounterID` 的部分按 `Date` 排序。

属于不同分区的数据还会被分成不同的分片，ClickHouse 通过在后台合并数据分片以便更有效的存储。属于不同分区的数据分片不会合并。这个合并机制并不保证相同主键的所有行都会合并到同一个数据分片中。

对于每个数据分片，ClickHouse会创建一个索引文件，索引文件包含每个索引行（『标记』）的主键值。索引行号定义为 `n * index_granularity` 。最大的 `n` 等于总行数除以`index_granularity`的值的整数部分。对于每列，也会记录跟主键相同的索引行到『标记』中。这些『标记』让你可以直接找到数据所在的列。
For each data part, ClickHouse creates an index file that contains the primary key value for each index row ("mark"). Index row numbers are defined as `n * index_granularity`. The maximum value `n` is equal to the integer part of dividing the total number of rows by the `index_granularity`. For each column, the "marks" are also written for the same index rows as the primary key. These "marks" allow you to find the data directly in the columns.

因为你可以只用单一的大表并不断地向里面加入一块块地数据 – `MergeTree` 引擎的就是为了这样的场景。

## 主键和索引在查询中的表现 {#primary-keys-and-indexes-in-queriesko

我们以 `(CounterID, Date)` 以主键。这样，排序好的索引的图示会是下面这样：

```
全部数据  :     [-------------------------------------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
标记:            |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
标记号:          0      1      2      3      4      5      6      7      8      9      10
```

如果指定查询如下：

- `CounterID in ('a', 'h')`，服务器会读取标记号在 `[0, 3)` 和 `[6, 8)` 区间中的数据。
- `CounterID IN ('a', 'h') AND Date = 3`，服务器会读取标记号在 `[1, 3)` 和 `[7, 8)` 区间中的数据。
- `Date = 3`，服务器会读取标记号在 `[1, 10]` 区间中的数据。

从上面例子可以看出使用索引查询总会比全表描述要高效。

稀疏索引会引起额外的数据读取。当读取主键中的一个区间范围时，每个数据块中至多数量为 `index_granularity * 2` 的额外的行会被读取。大部分情况下，当 `index_granularity = 8192` 时，ClickHouse的性能并不会降级。
A sparse index allows extra strings to be read. When reading a single range of the primary key, up to `index_granularity * 2` extra rows in each data block can be read. In most cases, ClickHouse performance does not degrade when `index_granularity = 8192`.

稀疏索引让你可以使用一个行数非常巨大的表。因为这样的索引总是存储在内存（RAM）中。

ClickHouse并不要求主键惟一。所以，你可以插入多条相关主键的行。

### 主键的选择

主键中列的数量并没有明确的限制。依据数据结构，你可以让主键包含更多或更少的列。这样可能会：

- 改善索引的性能。

    如果当前主键是 `(a, b)` ，然后加入另一个 `c` 列，若满足下面条件，则可以改善性能：
    - 有些查询带有列`c`的条件。
    - 很长的数据范围（ `index_granularity` 的数倍）里 `(a, b)` 都是相同的值，并且这样的情况很普遍。换种说法就是，当加入另一个列时，可以让你的查询跳过一些十分长的数据落雷。

- 改善数据压缩.

    ClickHouse 以主键排序分片数据，所以，数据的一致性越高，压缩越好。

- 在 [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) 和 [SummingMergeTree](summingmergetree.md) 引擎里，当数据合并时，会提供额外的处理逻辑。

    在这种情况下，指定一个跟主键不同的 *排序键* 是说得通的。

长的主键会对插入性能和内存消耗有负面影响，但主键中额外的列并不会在 `SELECT` 查询时影响 ClickHouse 性能。


### 选择不跟排序键不一样主键

指定一个跟排序键（用于在数据分片中排序行的表达式）主键（用于计算每个写到索引文件的标记值的表达式）是可以的。
这种情况下，主键表达式元组必须是排序键表达式元组的一个前缀。

这个特性对于使用 [SummingMergeTree](summingmergetree.md) 和
[AggregatingMergeTree](aggregatingmergetree.md) 表引擎是非常有用的。通常情况下，当使用这些引擎时，表里会有两类列：*维度* 和 *度量* 。典型的查询是在 `GROUP BY` 并过虑维度的情况下统计度量列的值。像 SummingMergeTree 和 AggregatingMergeTree ，用相同的排序键值统计行时，通常会加上所有的维度。造成的结果就是，这键的表达式就会由一长串的列组成，并且这长串的列还会因为新加维度而频繁更新。

这种情况下，主键中只保留一小部分的列就好，剩下的维度列加到排序键元组里，这样会提供高效的范围扫描性能。
In this case it makes sense to leave only a few columns in the primary key that will provide efficient
range scans and add the remaining dimension columns to the sorting key tuple.

[排序键的修改](../../query_language/alter.md) 是轻量级的操作，因为当一个新列同时被加入到表中和排序键中时，数据分片并不需要修改（剩下的部分用新的排序键表达式排好就可以了）

### Use of Indexes and Partitions in Queries

对于 `SELECT` 查询，ClickHouse分析索引是否可用。当 `WHERE/PREWHERE` 子句有一个表示等式或比较的表达式（可以是谓词链接的一个子项或也可是整个表达式），或者它有 `IN` 或 `LIKE` 作用在 主键或分区键的列或表达式 的一个固定前缀，或者这些列的某些部分重复的函数，或者表达式的逻辑关系。TODO

For `SELECT` queries, ClickHouse analyzes whether an index can be used. An index can be used if the `WHERE/PREWHERE` clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has `IN` or `LIKE` with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

因为，基于索引键上一个或多个区间运行快速的查询都是可能的。这个例子中，对于指定的跟踪标签；对于指定的标签和日期范围；对于指定的标签和日期；对于多个标签和日期范围等等上查询都会运行得非常快。
Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag; for a specific tag and date range; for a specific tag and date; for multiple tags with a date range, and so on.

当引擎配置如下时：
Let's look at the engine configured as follows:

```
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192
```

这种情况下，这些查询中：
In this case, in queries:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse 会用主键索引来裁剪掉不需要的数据，用按月分区的分区键裁剪掉那些只有不需要的数据的分区。
ClickHouse will use the primary key index to trim improper data and the monthly partitioning key to trim partitions that are in improper date ranges.

下面这个查询展示了，即使在复杂表达式的情况下，索引依然被使用了。但因为读表的操作是组织有序的，所以，这样使用索引并不会比全表扫描慢。
The queries above show that the index is used even for complex expressions. Reading from the table is organized so that using the index can't be slower than a full scan.

下面这个例子中，不会使用索引。
In the example below, the index can't be used.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

要检查 ClickHouse 执行一个查询时能是用索引，使用设置 [force_index_by_date](../settings/settings.md#settings-force_index_by_date) 和 [force_primary_key](../settings/settings.md) 。
To check whether ClickHouse can use the index when running a query, use the settings [force_index_by_date](../settings/settings.md#settings-force_index_by_date) and [force_primary_key](../settings/settings.md).

按月分区的键使得只需读取那些包含了需要的范围的数据的数据块。这种情况下，数据块会包含很多日期（最多一整月）的数据。在数据块中，数据按主键排序，可能第一列并不包含日期。因此，查询时只有指定日期条件，不指定主键前缀相关条件会比只用单个日期造成更多的数据读取。TODO
The key for partitioning by month allows reading only those data blocks which contain dates from the proper range. In this case, the data block may contain data for many dates (up to an entire month). Within a block, data is sorted by primary key, which might not contain the date as the first column. Because of this, using a query with only a date condition that does not specify the primary key prefix will cause more data to be read than for a single date.


### 数据跳跃索引（实验性的）

使用这类索引需要设置 `allow_experimental_data_skipping_indices` 为 1 。（执行 `SET allow_experimental_data_skipping_indices = 1`）

索引定义需要在 `CREATE` 语句的列定义列表里。
Index declaration in the columns section of the `CREATE` query.
```sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

`*MergeTree` 家族的表都能指定数据跳跃索引。
For tables from the `*MergeTree` family data skipping indices can be specified.

这些索引聚合一些关于块上的指定表达式的信息，
These indices aggregate some information about the specified expression on blocks, which consist of `granularity_value` granules (size of the granule is specified using `index_granularity` setting in the table engine),
然后，这些聚合用在 `SELECT` 查询时为了降低从磁盘总的数据读取量，通过跳过那些`where` 语句不能满足的 数据中的块
then these aggregates are used in `SELECT` queries for reducing the amount of data to read from the disk by skipping big blocks of data where `where` query cannot be satisfied.


示例
```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

下面的查询中，这个例子中的索引可以让ClickHouse使用来减少从磁盘读取的数据总量。
Indices from the example can be used by ClickHouse to reduce the amount of data to read from disk in following queries.
```sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### 索引的可用类型

* `minmax`
存储指定表达式的极值（如果表达式是 `tuple` ），那么会存储 `tuple` 中每个元素的极值，使用这样的存储的信息来跳过数据块跟主键一样。
Stores extremes of the specified expression (if the expression is `tuple`, then it stores extremes for each element of `tuple`), uses stored info for skipping blocks of the data like the primary key.

* `set(max_rows)`
存储指定表达式的惟一值（不超过 `max_rows` 行，`max_rows=0` 则表示『没有限制』）。使用这些可以检查 `WHERE` 表达式是否满足某个数据块。
Stores unique values of the specified expression (no more than `max_rows` rows, `max_rows=0` means "no limits"), use them to check if the `WHERE` expression is not satisfiable on a block of the data.  

* `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)` 
存储包含数据块中所有 n 元短语的 [布隆过滤器](https://en.wikipedia.org/wiki/Bloom_filter) 。可能在字符串上使用。
可以用于 `equals` ， `like` 和 `in` 表达式 的优化。
Stores [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that contains all ngrams from block of data. Works only with strings.
Can be used for optimization of `equals`, `like` and `in` expressions.
`n` -- 短语大小。
`n` -- ngram size,
`size_of_bloom_filter_in_bytes` -- 布隆过滤器大小，单位字节。（这里可以指定一个比较大的值，例如256或512，因为它会被压缩得很好）
`size_of_bloom_filter_in_bytes` -- bloom filter size in bytes (you can use big values here, for example, 256 or 512, because it can be compressed well),
`number_of_hash_functions` -- 用在布隆过滤器中的哈希函数的个数。
`number_of_hash_functions` -- number of hash functions used in bloom filter,
`random_seed` -- hash函数的随机种子。
`random_seed` -- seed for bloom filter hash functions.

* `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)` 
跟 `ngrambf_v1` 差不多，不同于 ngrams 存储任意指定长度的短词。它存储被字母数据分割的短句。
, but instead of ngrams stores tokens, which are sequences separated by non-alphanumeric characters.

```sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```


## 并发数据访问

对于表的并发访问，我们使用多版本机制。换言之，当同时读和更新一个表时，数据
For concurrent table access, we use multi-versioning. In other words, when a table is simultaneously read and updated, data is read from a set of parts that is current at the time of the query. There are no lengthy locks. Inserts do not get in the way of read operations.
从一个集合的分片 里读到的数据 就是当前这个时间查询到的
这里的操作不会有漫长的锁。插入不会阻塞读操作。

对表的读操作是自动并行的。


[来源文章](https://clickhouse.yandex/docs/en/operations/table_engines/mergetree/) <!--hide-->
