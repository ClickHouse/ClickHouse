# system.distributed_ddl_queue {#system_tables-distributed_ddl_queue}

包含有关在集群上执行的[分布式ddl查询(集群环境)](../../sql-reference/distributed-ddl.md)的信息.

列信息:

-   `entry` ([String](../../sql-reference/data-types/string.md)) — 查询ID.
-   `host_name` ([String](../../sql-reference/data-types/string.md)) — 主机名称.
-   `host_address` ([String](../../sql-reference/data-types/string.md)) — 主机名解析到的IP地址.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — 主机端口.
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — 查询状态.
-   `cluster` ([String](../../sql-reference/data-types/string.md)) — 群集名称.
-   `query` ([String](../../sql-reference/data-types/string.md)) — 执行查询.
-   `initiator` ([String](../../sql-reference/data-types/string.md)) — 执行查询的节点.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询开始时间.
-   `query_finish_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询结束时间.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 查询执行时间(毫秒).
-   `exception_code` ([Enum8](../../sql-reference/data-types/enum.md)) — 来自于[ZooKeeper](../../operations/tips.md#zookeeper)的异常代码.

**示例**

``` sql
SELECT *
FROM system.distributed_ddl_queue
WHERE cluster = 'test_cluster'
LIMIT 2
FORMAT Vertical

Query id: f544e72a-6641-43f1-836b-24baa1c9632a

Row 1:
──────
entry:             query-0000000000
host_name:         clickhouse01
host_address:      172.23.0.11
port:              9000
status:            Finished
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
initiator:         clickhouse01:9000
query_start_time:  2020-12-30 13:07:51
query_finish_time: 2020-12-30 13:07:51
query_duration_ms: 6
exception_code:    ZOK

Row 2:
──────
entry:             query-0000000000
host_name:         clickhouse02
host_address:      172.23.0.12
port:              9000
status:            Finished
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
initiator:         clickhouse01:9000
query_start_time:  2020-12-30 13:07:51
query_finish_time: 2020-12-30 13:07:51
query_duration_ms: 6
exception_code:    ZOK

2 rows in set. Elapsed: 0.025 sec.
```

[原始文章](https://clickhouse.com/docs/en/operations/system_tables/distributed_ddl_queuedistributed_ddl_queue.md) <!--hide-->
