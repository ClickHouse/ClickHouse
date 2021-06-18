# ClickHouse的特性 {#clickhouse-de-te-xing}

## 真正的列式数据库管理系统 {#zhen-zheng-de-lie-shi-shu-ju-ku-guan-li-xi-tong}

在一个真正的列式数据库管理系统中，除了数据本身外不应该存在其他额外的数据。这意味着为了避免在值旁边存储它们的长度«number»，你必须支持固定长度数值类型。例如，10亿个UInt8类型的数据在未压缩的情况下大约消耗1GB左右的空间，如果不是这样的话，这将对CPU的使用产生强烈影响。即使是在未压缩的情况下，紧凑的存储数据也是非常重要的，因为解压缩的速度主要取决于未压缩数据的大小。

这是非常值得注意的，因为在一些其他系统中也可以将不同的列分别进行存储，但由于对其他场景进行的优化，使其无法有效的处理分析查询。例如： HBase，BigTable，Cassandra，HyperTable。在这些系统中，你可以得到每秒数十万的吞吐能力，但是无法得到每秒几亿行的吞吐能力。

需要说明的是，ClickHouse不单单是一个数据库， 它是一个数据库管理系统。因为它允许在运行时创建表和数据库、加载数据和运行查询，而无需重新配置或重启服务。

## 数据压缩 {#shu-ju-ya-suo}

在一些列式数据库管理系统中(例如：InfiniDB CE 和 MonetDB) 并没有使用数据压缩。但是, 若想达到比较优异的性能，数据压缩确实起到了至关重要的作用。

## 数据的磁盘存储 {#shu-ju-de-ci-pan-cun-chu}

许多的列式数据库(如 SAP HANA, Google PowerDrill)只能在内存中工作，这种方式会造成比实际更多的设备预算。ClickHouse被设计用于工作在传统磁盘上的系统，它提供每GB更低的存储成本，但如果有可以使用SSD和内存，它也会合理的利用这些资源。

## 多核心并行处理 {#duo-he-xin-bing-xing-chu-li}

ClickHouse会使用服务器上一切可用的资源，从而以最自然的方式并行处理大型查询。

## 多服务器分布式处理 {#duo-fu-wu-qi-fen-bu-shi-chu-li}

上面提到的列式数据库管理系统中，几乎没有一个支持分布式的查询处理。
在ClickHouse中，数据可以保存在不同的shard上，每一个shard都由一组用于容错的replica组成，查询可以并行地在所有shard上进行处理。这些对用户来说是透明的

## 支持SQL {#zhi-chi-sql}

ClickHouse支持基于SQL的声明式查询语言，该语言大部分情况下是与SQL标准兼容的。
支持的查询包括 GROUP BY，ORDER BY，IN，JOIN以及非相关子查询。
不支持窗口函数和相关子查询。

## 向量引擎 {#xiang-liang-yin-qing}

为了高效的使用CPU，数据不仅仅按列存储，同时还按向量(列的一部分)进行处理，这样可以更加高效地使用CPU。

## 实时的数据更新 {#shi-shi-de-shu-ju-geng-xin}

ClickHouse支持在表中定义主键。为了使查询能够快速在主键中进行范围查找，数据总是以增量的方式有序的存储在MergeTree中。因此，数据可以持续不断地高效的写入到表中，并且写入的过程中不会存在任何加锁的行为。

## 索引 {#suo-yin}

按照主键对数据进行排序，这将帮助ClickHouse在几十毫秒以内完成对数据特定值或范围的查找。

## 适合在线查询 {#gua-he-zai-xian-cha-xun}

在线查询意味着在没有对数据做任何预处理的情况下以极低的延迟处理查询并将结果加载到用户的页面中。

## 支持近似计算 {#zhi-chi-jin-si-ji-suan}

ClickHouse提供各种各样在允许牺牲数据精度的情况下对查询进行加速的方法：

1.  用于近似计算的各类聚合函数，如：distinct values, medians, quantiles
2.  基于数据的部分样本进行近似查询。这时，仅会从磁盘检索少部分比例的数据。
3.  不使用全部的聚合条件，通过随机选择有限个数据聚合条件进行聚合。这在数据聚合条件满足某些分布条件下，在提供相当准确的聚合结果的同时降低了计算资源的使用。

## 支持数据复制和数据完整性 {#zhi-chi-shu-ju-fu-zhi-he-shu-ju-wan-zheng-xing}

ClickHouse使用异步的多主复制技术。当数据被写入任何一个可用副本后，系统会在后台将数据分发给其他副本，以保证系统在不同副本上保持相同的数据。在大多数情况下ClickHouse能在故障后自动恢复，在一些少数的复杂情况下需要手动恢复。

更多信息，参见 [数据复制](../engines/table-engines/mergetree-family/replication.md)。

# 限制 {#clickhouseke-xian-zhi}

1.  没有完整的事务支持。
2.  缺少高频率，低延迟的修改或删除已存在数据的能力。仅能用于批量删除或修改数据，但这符合 [GDPR](https://gdpr-info.eu)。
3.  稀疏索引使得ClickHouse不适合通过其键检索单行的点查询。

[来源文章](https://clickhouse.tech/docs/en/introduction/distinctive_features/) <!--hide-->
