# system.distributed_ddl_queue {#system_tables-distributed_ddl_queue}

Contains information about distributed ddl queries (ON CLUSTER queries) that were executed on a cluster.

Columns:

-   `entry`  ([String](../../sql-reference/data-types/string.md)) - Query id.
-   `host_name`  ([String](../../sql-reference/data-types/string.md)) - Hostname.
-   `host_address`  ([String](../../sql-reference/data-types/string.md)) - IP address that the Hostname resolves to.
-   `port`  ([UInt16](../../sql-reference/data-types/int-uint.md)) - Host Port.
-   `status`  ([Enum](../../sql-reference/data-types/enum.md)) - Stats of the query.
-   `cluster`  ([String](../../sql-reference/data-types/string.md)) - Cluster name.
-   `value`  ([String](../../sql-reference/data-types/string.md)) - Node value.
-   `query_start_time` ([Date](../../sql-reference/data-types/date.md)) — Query start time.
-   `query_finish_time` ([Date](../../sql-reference/data-types/date.md)) — Query finish time.
-   `query_duration_ms` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Duration of query execution in milliseconds.
-   `exception_code`  ([Enum](../../sql-reference/data-types/enum.md)) - Exception code from ZooKeeper.


**Example**

``` sql
SELECT *
FROM system.distributed_ddl_queue
WHERE (cluster = 'test_cluster') AND (entry = 'query-0000000000')
LIMIT 2
FORMAT Vertical

Query id: 856f0efd-07c9-4f1a-815f-a19112df5bb3

Row 1:
──────
entry:             query-0000000000
host_name:         clickhouse01
host_address:      172.23.0.11
port:              9000
status:            finished
cluster:           test_cluster
value:            version: 1
query: CREATE DATABASE test_db UUID '40ac7692-70d3-48a9-bc29-4ade18957f59' ON CLUSTER test_cluster
hosts: ['clickhouse01:9000','clickhouse02:9000','clickhouse03:9000','clickhouse04:9000']
initiator: clickhouse01:9000

query_start_time:  2020-12-15 10:06:35
query_finish_time: 2020-12-15 10:06:35
query_duration_ms: 7
exception_code:    ZOK

Row 2:
──────
entry:             query-0000000000
host_name:         clickhouse02
host_address:      172.23.0.12
port:              9000
status:            finished
cluster:           test_cluster
value:            version: 1
query: CREATE DATABASE test_db UUID '40ac7692-70d3-48a9-bc29-4ade18957f59' ON CLUSTER test_cluster
hosts: ['clickhouse01:9000','clickhouse02:9000','clickhouse03:9000','clickhouse04:9000']
initiator: clickhouse01:9000

query_start_time:  2020-12-15 10:06:35
query_finish_time: 2020-12-15 10:06:35
query_duration_ms: 7
exception_code:    ZOK

2 rows in set. Elapsed: 0.032 sec. 
```


[Original article](https://clickhouse.tech/docs/en/operations/system_tables/distributed_ddl_queuedistributed_ddl_queue.md) <!--hide-->
 