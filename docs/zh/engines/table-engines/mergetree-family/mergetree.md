# MergeTree {#table_engines-mergetree}

Clickhouse 中最强大的表引擎当属 `MergeTree` （合并树）引擎及该系列（`*MergeTree`）中的其他引擎。

`MergeTree` 引擎系列的基本理念如下。当你有巨量数据要插入到表中，你要高效地一批批写入数据片段，并希望这些数据片段在后台按照一定规则合并。相比在插入时不断修改（重写）数据进存储，这种策略会高效很多。

主要特点:

-   存储的数据按主键排序。

        这让你可以创建一个用于快速检索数据的小稀疏索引。

-   允许使用分区，如果指定了 [分区键](custom-partitioning-key.md) 的话。

        在相同数据集和相同结果集的情况下 ClickHouse 中某些带分区的操作会比普通操作更快。查询中指定了分区键时 ClickHouse 会自动截取分区数据。这也有效增加了查询性能。

-   支持数据副本。

        `ReplicatedMergeTree` 系列的表便是用于此。更多信息，请参阅 [数据副本](replication.md) 一节。

-   支持数据采样。

        需要的话，你可以给表设置一个采样方法。

!!! 注意 "注意"
    [合并](../special/merge.md#merge) 引擎并不属于 `*MergeTree` 系列。

## 建表 {#table_engine-mergetree-creating-a-table}

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

请求参数的描述，参考 [请求描述](../../../engines/table-engines/mergetree-family/mergetree.md) 。

<a name="mergetree-query-clauses"></a>

**子句**

-   `ENGINE` - 引擎名和参数。 `ENGINE = MergeTree()`. `MergeTree` 引擎没有参数。

-   `PARTITION BY` — [分区键](custom-partitioning-key.md) 。

        要按月分区，可以使用表达式 `toYYYYMM(date_column)` ，这里的 `date_column` 是一个 [Date](../../../engines/table_engines/mergetree_family/mergetree.md) 类型的列。这里该分区名格式会是 `"YYYYMM"` 这样。

-   `ORDER BY` — 表的排序键。

        可以是一组列的元组或任意的表达式。 例如: `ORDER BY (CounterID, EventDate)` 。

-   `PRIMARY KEY` - 主键，如果要设成 [跟排序键不相同](#xuan-ze-gen-pai-xu-jian-bu-yi-yang-zhu-jian)。

        默认情况下主键跟排序键（由 `ORDER BY` 子句指定）相同。
        因此，大部分情况下不需要再专门指定一个 `PRIMARY KEY` 子句。

-   `SAMPLE BY` — 用于抽样的表达式。

        如果要用抽样表达式，主键中必须包含这个表达式。例如：
        `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))` 。

-   `SETTINGS` — 影响 `MergeTree` 性能的额外参数：

    -   `index_granularity` — 索引粒度。即索引中相邻『标记』间的数据行数。默认值，8192 。该列表中所有可用的参数可以从这里查看 [MergeTreeSettings.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/MergeTree/MergeTreeSettings.h) 。
    -   `index_granularity_bytes` — 索引粒度，以字节为单位，默认值: 10Mb。如果仅按数据行数限制索引粒度, 请设置为0(不建议)。
    -   `enable_mixed_granularity_parts` — 启用或禁用通过 `index_granularity_bytes` 控制索引粒度的大小。在19.11版本之前, 只有 `index_granularity` 配置能够用于限制索引粒度的大小。当从大表(数十或数百兆)中查询数据时候，`index_granularity_bytes` 配置能够提升ClickHouse的性能。如果你的表内数据量很大，可以开启这项配置用以提升`SELECT` 查询的性能。
    -   `use_minimalistic_part_header_in_zookeeper` — 数据片段头在 ZooKeeper 中的存储方式。如果设置了 `use_minimalistic_part_header_in_zookeeper=1` ，ZooKeeper 会存储更少的数据。更多信息参考『服务配置参数』这章中的 [设置描述](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) 。
    -   `min_merge_bytes_to_use_direct_io` — 使用直接 I/O 来操作磁盘的合并操作时要求的最小数据量。合并数据片段时，ClickHouse 会计算要被合并的所有数据的总存储空间。如果大小超过了 `min_merge_bytes_to_use_direct_io` 设置的字节数，则 ClickHouse 将使用直接 I/O 接口（`O_DIRECT` 选项）对磁盘读写。如果设置 `min_merge_bytes_to_use_direct_io = 0` ，则会禁用直接 I/O。默认值：`10 * 1024 * 1024 * 1024` 字节。
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — TTL合并频率的最小间隔时间。默认值: 86400 (1 天)。
    -   `write_final_mark` — 启用或禁用在数据片段尾部写入最终索引标记。默认值: 1（不建议更改）。
    -   `storage_policy` — 存储策略。 参见 [使用多个区块装置进行数据存储](#table_engine-mergetree-multiple-volumes).

**示例配置**

    ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192

示例中，我们设为按月分区。

同时我们设置了一个按用户ID哈希的抽样表达式。这让你可以有该表中每个 `CounterID` 和 `EventDate` 下面的数据的伪随机分布。如果你在查询时指定了 [SAMPLE](../../../engines/table-engines/mergetree-family/mergetree.md#select-sample-clause) 子句。 ClickHouse会返回对于用户子集的一个均匀的伪随机数据采样。

`index_granularity` 可省略，默认值为 8192 。

<details markdown="1">

<summary>已弃用的建表方法</summary>

!!! attention "注意"
    不要在新版项目中使用该方法，可能的话，请将旧项目切换到上述方法。

    CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
    (
        name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
        name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
        ...
    ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)

**MergeTree() 参数**

-   `date-column` — 类型为 [日期](../../../engines/table-engines/mergetree-family/mergetree.md) 的列名。ClickHouse 会自动依据这个列按月创建分区。分区名格式为 `"YYYYMM"` 。
-   `sampling_expression` — 采样表达式。
-   `(primary, key)` — 主键。类型 — [元组()](../../../engines/table-engines/mergetree-family/mergetree.md)
-   `index_granularity` — 索引粒度。即索引中相邻『标记』间的数据行数。设为 8192 可以适用大部分场景。

**示例**

    MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)

对于主要的配置方法，这里 `MergeTree` 引擎跟前面的例子一样，可以以同样的方式配置。
</details>

## 数据存储 {#mergetree-data-storage}

表由按主键排序的数据 *片段* 组成。

当数据被插入到表中时，会分成数据片段并按主键的字典序排序。例如，主键是 `(CounterID, Date)` 时，片段中数据按 `CounterID` 排序，具有相同 `CounterID` 的部分按 `Date` 排序。

不同分区的数据会被分成不同的片段，ClickHouse 在后台合并数据片段以便更高效存储。不会合并来自不同分区的数据片段。这个合并机制并不保证相同主键的所有行都会合并到同一个数据片段中。

ClickHouse 会为每个数据片段创建一个索引文件，索引文件包含每个索引行（『标记』）的主键值。索引行号定义为 `n * index_granularity` 。最大的 `n` 等于总行数除以 `index_granularity` 的值的整数部分。对于每列，跟主键相同的索引行处也会写入『标记』。这些『标记』让你可以直接找到数据所在的列。

你可以只用一单一大表并不断地一块块往里面加入数据 – `MergeTree` 引擎的就是为了这样的场景。

## 主键和索引在查询中的表现 {#primary-keys-and-indexes-in-queries}

我们以 `(CounterID, Date)` 以主键。排序好的索引的图示会是下面这样：

    全部数据  :     [-------------------------------------------------------------------------]
    CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
    Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
    标记:            |      |      |      |      |      |      |      |      |      |      |
                    a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
    标记号:          0      1      2      3      4      5      6      7      8      9      10

如果指定查询如下：

-   `CounterID in ('a', 'h')`，服务器会读取标记号在 `[0, 3)` 和 `[6, 8)` 区间中的数据。
-   `CounterID IN ('a', 'h') AND Date = 3`，服务器会读取标记号在 `[1, 3)` 和 `[7, 8)` 区间中的数据。
-   `Date = 3`，服务器会读取标记号在 `[1, 10]` 区间中的数据。

上面例子可以看出使用索引通常会比全表描述要高效。

稀疏索引会引起额外的数据读取。当读取主键单个区间范围的数据时，每个数据块中最多会多读 `index_granularity * 2` 行额外的数据。大部分情况下，当 `index_granularity = 8192` 时，ClickHouse的性能并不会降级。

稀疏索引让你能操作有巨量行的表。因为这些索引是常驻内存（RAM）的。

ClickHouse 不要求主键惟一。所以，你可以插入多条具有相同主键的行。

### 主键的选择 {#zhu-jian-de-xuan-ze}

主键中列的数量并没有明确的限制。依据数据结构，你应该让主键包含多些或少些列。这样可以：

-   改善索引的性能。

        如果当前主键是 `(a, b)` ，然后加入另一个 `c` 列，满足下面条件时，则可以改善性能：
        - 有带有 `c` 列条件的查询。
        - 很长的数据范围（ `index_granularity` 的数倍）里 `(a, b)` 都是相同的值，并且这种的情况很普遍。换言之，就是加入另一列后，可以让你的查询略过很长的数据范围。

-   改善数据压缩。

        ClickHouse 以主键排序片段数据，所以，数据的一致性越高，压缩越好。

-   [折叠树](collapsingmergetree.md#table_engine-collapsingmergetree) 和 [SummingMergeTree](summingmergetree.md) 引擎里，数据合并时，会有额外的处理逻辑。

        在这种情况下，指定一个跟主键不同的 *排序键* 也是有意义的。

长的主键会对插入性能和内存消耗有负面影响，但主键中额外的列并不影响 `SELECT` 查询的性能。

### 选择跟排序键不一样主键 {#xuan-ze-gen-pai-xu-jian-bu-yi-yang-zhu-jian}

指定一个跟排序键（用于排序数据片段中行的表达式）
不一样的主键（用于计算写到索引文件的每个标记值的表达式）是可以的。
这种情况下，主键表达式元组必须是排序键表达式元组的一个前缀。

当使用 [SummingMergeTree](summingmergetree.md) 和
[AggregatingMergeTree](aggregatingmergetree.md) 引擎时，这个特性非常有用。
通常，使用这类引擎时，表里列分两种：*维度* 和 *度量* 。
典型的查询是在 `GROUP BY` 并过虑维度的情况下统计度量列的值。
像 SummingMergeTree 和 AggregatingMergeTree ，用相同的排序键值统计行时，
通常会加上所有的维度。结果就是，这键的表达式会是一长串的列组成，
并且这组列还会因为新加维度必须频繁更新。

这种情况下，主键中仅预留少量列保证高效范围扫描，
剩下的维度列放到排序键元组里。这样是合理的。

[排序键的修改](../../../engines/table-engines/mergetree-family/mergetree.md) 是轻量级的操作，因为一个新列同时被加入到表里和排序键后时，已存在的数据片段并不需要修改。由于旧的排序键是新排序键的前缀，并且刚刚添加的列中没有数据，因此在表修改时的数据对于新旧的排序键来说都是有序的。

### 索引和分区在查询中的应用 {#suo-yin-he-fen-qu-zai-cha-xun-zhong-de-ying-yong}

对于 `SELECT` 查询，ClickHouse 分析是否可以使用索引。如果 `WHERE/PREWHERE` 子句具有下面这些表达式（作为谓词链接一子项或整个）则可以使用索引：基于主键或分区键的列或表达式的部分的等式或比较运算表达式；基于主键或分区键的列或表达式的固定前缀的 `IN` 或 `LIKE` 表达式；基于主键或分区键的列的某些函数；基于主键或分区键的表达式的逻辑表达式。 <!-- It is too hard for me to translate this section as the original text completely. So I did it with my own understanding. If you have good idea, please help me. -->

因此，在索引键的一个或多个区间上快速地跑查询都是可能的。下面例子中，指定标签；指定标签和日期范围；指定标签和日期；指定多个标签和日期范围等运行查询，都会非常快。

当引擎配置如下时：

    ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192

这种情况下，这些查询：

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse 会依据主键索引剪掉不符合的数据，依据按月分区的分区键剪掉那些不包含符合数据的分区。

上文的查询显示，即使索引用于复杂表达式。因为读表操作是组织好的，所以，使用索引不会比完整扫描慢。

下面这个例子中，不会使用索引。

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

要检查 ClickHouse 执行一个查询时能否使用索引，可设置 [force\_index\_by\_date](../../../operations/settings/settings.md#settings-force_index_by_date) 和 [force\_primary\_key](../../../operations/settings/settings.md) 。

按月分区的分区键是只能读取包含适当范围日期的数据块。这种情况下，数据块会包含很多天（最多整月）的数据。在块中，数据按主键排序，主键第一列可能不包含日期。因此，仅使用日期而没有带主键前缀条件的查询将会导致读取超过这个日期范围。

### 跳数索引（分段汇总索引，实验性的） {#tiao-shu-suo-yin-fen-duan-hui-zong-suo-yin-shi-yan-xing-de}

需要设置 `allow_experimental_data_skipping_indices` 为 1 才能使用此索引。（执行 `SET allow_experimental_data_skipping_indices = 1`）。

此索引在 `CREATE` 语句的列部分里定义。

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

`*MergeTree` 系列的表都能指定跳数索引。

这些索引是由数据块按粒度分割后的每部分在指定表达式上汇总信息 `granularity_value` 组成（粒度大小用表引擎里 `index_granularity` 的指定）。
这些汇总信息有助于用 `where` 语句跳过大片不满足的数据，从而减少 `SELECT` 查询从磁盘读取的数据量，

示例

``` sql
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

上例中的索引能让 ClickHouse 执行下面这些查询时减少读取数据量。

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### 索引的可用类型 {#table_engine-mergetree-data_skipping-indexes}

-   `minmax`
    存储指定表达式的极值（如果表达式是 `tuple` ，则存储 `tuple` 中每个元素的极值），这些信息用于跳过数据块，类似主键。

-   `set(max_rows)`
    存储指定表达式的惟一值（不超过 `max_rows` 个，`max_rows=0` 则表示『无限制』）。这些信息可用于检查 `WHERE` 表达式是否满足某个数据块。

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`
    存储包含数据块中所有 n 元短语的 [布隆过滤器](https://en.wikipedia.org/wiki/Bloom_filter) 。只可用在字符串上。
    可用于优化 `equals` ， `like` 和 `in` 表达式的性能。
    `n` – 短语长度。
    `size_of_bloom_filter_in_bytes` – 布隆过滤器大小，单位字节。（因为压缩得好，可以指定比较大的值，如256或512）。
    `number_of_hash_functions` – 布隆过滤器中使用的 hash 函数的个数。
    `random_seed` – hash 函数的随机种子。

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`
    跟 `ngrambf_v1` 类似，不同于 ngrams 存储字符串指定长度的所有片段。它只存储被非字母数据字符分割的片段。

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

## 并发数据访问 {#bing-fa-shu-ju-fang-wen}

应对表的并发访问，我们使用多版本机制。换言之，当同时读和更新表时，数据从当前查询到的一组片段中读取。没有冗长的的锁。插入不会阻碍读取。

对表的读操作是自动并行的。

## 列和表的TTL {#table_engine-mergetree-ttl}

TTL可以设置值的生命周期，它既可以为整张表设置，也可以为每个列字段单独设置。如果`TTL`同时作用于表和字段，ClickHouse会使用先到期的那个。

被设置TTL的表，必须拥有[日期](../../../engines/table-engines/mergetree-family/mergetree.md) 或 [日期时间](../../../engines/table-engines/mergetree-family/mergetree.md) 类型的字段。要定义数据的生命周期，需要在这个日期字段上使用操作符，例如:

``` sql
TTL time_column
TTL time_column + interval
```

要定义`interval`, 需要使用 [时间间隔](../../../engines/table-engines/mergetree-family/mergetree.md#operators-datetime) 操作符。

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### 列字段 TTL {#mergetree-column-ttl}

当列字段中的值过期时, ClickHouse会将它们替换成数据类型的默认值。如果分区内，某一列的所有值均已过期，则ClickHouse会从文件系统中删除这个分区目录下的列文件。

`TTL`子句不能被用于主键字段。

示例说明:

创建一张包含 `TTL` 的表

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

为表中已存在的列字段添加 `TTL`

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

修改列字段的 `TTL`

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### 表 TTL {#mergetree-table-ttl}

当表内的数据过期时, ClickHouse会删除所有对应的行。

举例说明:

创建一张包含 `TTL` 的表

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH;
```

修改表的 `TTL`

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**删除数据**

当ClickHouse合并数据分区时, 会删除TTL过期的数据。

当ClickHouse发现数据过期时, 它将会执行一个计划外的合并。要控制这类合并的频率, 你可以设置 `merge_with_ttl_timeout`。如果该值被设置的太低, 它将导致执行许多的计划外合并，这可能会消耗大量资源。

如果在合并的时候执行`SELECT` 查询, 则可能会得到过期的数据。为了避免这种情况，可以在`SELECT`之前使用 [OPTIMIZE](../../../engines/table-engines/mergetree-family/mergetree.md#misc_operations-optimize) 查询。

## 使用多个块设备进行数据存储 {#table_engine-mergetree-multiple-volumes}

### 配置 {#table_engine-mergetree-multiple-volumes_configure}

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/mergetree/) <!--hide-->
