# 表引擎

表引擎（即表的类型）决定了：

* 数据的存储方式和位置，写到哪里以及从哪里读取数据
* 支持哪些查询以及如何支持。
* 并发数据访问。
* 索引的使用（如果存在）。
* 是否可以执行多线程请求。
* 数据复制参数。

# 引擎类型
## MergeTree

适用于高负载任务的最通用和功能最强大的表引擎。这些引擎共同特点是可以快速插入数据并进行后续的后台数据处理。 MergeTree系列引擎支持数据复制（使用引擎的[Replicated*](https://clickhouse.yandex/docs/en/operations/table_engines/replication/) 版本），分区和其他引擎不支持的一些其他功能。

该类型的引擎：
* [MergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/mergetree/)
* [ReplacingMergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/replacingmergetree/)
* [SummingMergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/summingmergetree/)
* [AggregatingMergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/aggregatingmergetree/)
* [CollapsingMergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/collapsingmergetree/)
* [VersionedCollapsingMergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/versionedcollapsingmergetree/)
* [GraphiteMergeTree](https://clickhouse.yandex/docs/en/operations/table_engines/graphitemergetree/)

## Log
具有最小功能的[轻量级引擎](https://clickhouse.yandex/docs/en/operations/table_engines/log_family/)。当您需要快速写入许多小表（最多约100万行）并在以后整体读取它们时，它们是最有效的。

该类型的引擎：

* [TinyLog](https://clickhouse.yandex/docs/en/operations/table_engines/tinylog/）
* [StripeLog](https://clickhouse.yandex/docs/en/operations/table_engines/stripelog/）
* [Log](https://clickhouse.yandex/docs/en/operations/table_engines/log/）

## Intergation engines
用于与其他数据和处理系统进行通讯的引擎。
该类型的引擎：

* [Kafka](https://clickhouse.yandex/docs/en/operations/table_engines/kafka/)
* [MySQL](https://clickhouse.yandex/docs/en/operations/table_engines/mysql/)
* [ODBC](https://clickhouse.yandex/docs/en/operations/table_engines/odbc/)
* [JDBC](https://clickhouse.yandex/docs/en/operations/table_engines/jdbc/)
* [HDFS](https://clickhouse.yandex/docs/en/operations/table_engines/hdfs/)

## 特定引擎
该类型的引擎：

* [Distributed](https://clickhouse.yandex/docs/en/operations/table_engines/distributed/)
* [MaterializedView](https://clickhouse.yandex/docs/en/operations/table_engines/materializedview/)
* [Dictionary](https://clickhouse.yandex/docs/en/operations/table_engines/dictionary/)
* [Merge](https://clickhouse.yandex/docs/en/operations/table_engines/merge/)
* [File](https://clickhouse.yandex/docs/en/operations/table_engines/file/)
* [Null](https://clickhouse.yandex/docs/en/operations/table_engines/null/)
* [Set](https://clickhouse.yandex/docs/en/operations/table_engines/set/)
* [Join](https://clickhouse.yandex/docs/en/operations/table_engines/join/)
* [URL](https://clickhouse.yandex/docs/en/operations/table_engines/url/)
* [View](https://clickhouse.yandex/docs/en/operations/table_engines/view/)
* [Memory](https://clickhouse.yandex/docs/en/operations/table_engines/memory/)
* [Buffer](https://clickhouse.yandex/docs/en/operations/table_engines/buffer/)

#虚拟列
虚拟表列引擎源码中是不可或缺的表引擎属性。

你不应该在创建表查询中指定虚拟列,并且在你执行SHOW CREATE TABLE和DESCRIBE TABLE语句时，返回的结果中也不会包含虚拟列。虚拟列是只读的,所以你不能将数据插入到虚拟列中。

想要查询虚拟列中的数据，你必须在SELECT查询中指定它的名字。SELECT *语句不会返回虚拟列的内容。

若你创建的表中有一列与虚拟列的名字相同，那么虚拟列将不能再被访问。我们不建议这么做。为了避免这种列名的冲突，虚拟列的名字一般都以下滑线开头。
