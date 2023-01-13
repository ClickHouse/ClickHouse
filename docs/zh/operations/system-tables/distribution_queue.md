# system.distribution_queue {#system_tables-distribution_queue}

包含关于队列中要发送到分片的本地文件的信息. 这些本地文件包含通过以异步模式将新数据插入到Distributed表中而创建的新部分.

列信息:

-   `database` ([String](../../sql-reference/data-types/string.md)) — 数据库名称.

-   `table` ([String](../../sql-reference/data-types/string.md)) — 表名称.

-   `data_path` ([String](../../sql-reference/data-types/string.md)) — 存放本地文件的文件夹的路径.

-   `is_blocked` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag表示是否阻止向服务器发送本地文件.

-   `error_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 错误总数.

-   `data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 文件夹中的本地文件数.

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 本地文件中压缩数据的大小, 以字节为单位.

-   `broken_data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 被标记为损坏的文件数量(由于错误).

-   `broken_data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 破碎文件中压缩数据的大小, 以字节为单位.

-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — 关于最近发生的错误的文本信息(如果有的话).

**示例**

``` sql
SELECT * FROM system.distribution_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:              default
table:                 dist
data_path:             ./store/268/268bc070-3aad-4b1a-9cf2-4987580161af/default@127%2E0%2E0%2E2:9000/
is_blocked:            1
error_count:           0
data_files:            1
data_compressed_bytes: 499
last_exception:
```

**另请参阅**

-   [分布式表引擎](../../engines/table-engines/special/distributed.md)

[原始文章](https://clickhouse.com/docs/en/operations/system_tables/distribution_queue) <!--hide-->
