# distributed_ddl_queue

Contains information about [distributed ddl queries (ON CLUSTER clause)](../../sql-reference/distributed-ddl.md) that were executed on a cluster.

Columns:

-   `entry` ([String](../../sql-reference/data-types/string.md)) — Query id.
-   `host_name` ([String](../../sql-reference/data-types/string.md)) — Hostname.
-   `host_address` ([String](../../sql-reference/data-types/string.md)) — IP address that the Hostname resolves to.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — Host Port.
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Status of the query.
-   `cluster` ([String](../../sql-reference/data-types/string.md)) — Cluster name.
-   `query` ([String](../../sql-reference/data-types/string.md)) — Query executed.
-   `initiator` ([String](../../sql-reference/data-types/string.md)) — Node that executed the query.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query start time.
-   `query_finish_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query finish time.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Duration of query execution (in milliseconds).
-   `exception_code` ([Enum8](../../sql-reference/data-types/enum.md)) — Exception code from [ClickHouse Keeper](../../operations/tips.md#zookeeper).

**Example**

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

