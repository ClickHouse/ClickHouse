# system.replicated_fetches {#system_tables-replicated_fetches}

包含当前正在运行的后台提取的信息.

列信息:

-   `database` ([String](../../sql-reference/data-types/string.md)) — 数据库名称.

-   `table` ([String](../../sql-reference/data-types/string.md)) — 表名称.

-   `elapsed` ([Float64](../../sql-reference/data-types/float.md)) — 显示当前正在运行的后台提取开始以来经过的时间(以秒为单位).

-   `progress` ([Float64](../../sql-reference/data-types/float.md)) — 完成工作的百分比从0到1.

-   `result_part_name` ([String](../../sql-reference/data-types/string.md)) — 显示当前正在运行的后台提取的结果而形成的部分的名称.

-   `result_part_path` ([String](../../sql-reference/data-types/string.md)) — 显示当前正在运行的后台提取的结果而形成的部分的绝对路径.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — 分区 ID.

-   `total_size_bytes_compressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 结果部分中压缩数据的总大小(以字节为单位).

-   `bytes_read_compressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 从结果部分读取的压缩字节数.

-   `source_replica_path` ([String](../../sql-reference/data-types/string.md)) — 源副本的绝对路径.

-   `source_replica_hostname` ([String](../../sql-reference/data-types/string.md)) — 源副本的主机名称.

-   `source_replica_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — 源副本的端口号.

-   `interserver_scheme` ([String](../../sql-reference/data-types/string.md)) — Name of the interserver scheme.

-   `URI` ([String](../../sql-reference/data-types/string.md)) — 统一资源标识符.

-   `to_detached` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 该标志指示是否正在使用 `TO DETACHED` 表达式执行当前正在运行的后台提取.

-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 线程标识符.

**示例**

``` sql
SELECT * FROM system.replicated_fetches LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:                    default
table:                       t
elapsed:                     7.243039876
progress:                    0.41832135995612835
result_part_name:            all_0_0_0
result_part_path:            /var/lib/clickhouse/store/700/70080a04-b2de-4adf-9fa5-9ea210e81766/all_0_0_0/
partition_id:                all
total_size_bytes_compressed: 1052783726
bytes_read_compressed:       440401920
source_replica_path:         /clickhouse/test/t/replicas/1
source_replica_hostname:     node1
source_replica_port:         9009
interserver_scheme:          http
URI:                         http://node1:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftest%2Ft%2Freplicas%2F1&part=all_0_0_0&client_protocol_version=4&compress=false
to_detached:                 0
thread_id:                   54
```

**另请参阅**

-   [管理 ReplicatedMergeTree 表](../../sql-reference/statements/system/#query-language-system-replicated)

[原始文章](https://clickhouse.com/docs/en/operations/system_tables/replicated_fetches) <!--hide-->
