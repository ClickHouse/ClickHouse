---
slug: /en/operations/system-tables/distributed_ddl_queue
---
# distributed_ddl_queue

Contains information about [distributed ddl queries (ON CLUSTER clause)](../../sql-reference/distributed-ddl.md) that were executed on a cluster.

Columns:

- `entry` ([String](../../sql-reference/data-types/string.md)) — Query id.
- `entry_version` ([Nullable(UInt8)](../../sql-reference/data-types/int-uint.md)) - Version of the entry
- `initiator_host` ([Nullable(String)](../../sql-reference/data-types/string.md)) - Host that initiated the DDL operation
- `initiator_port` ([Nullable(UInt16)](../../sql-reference/data-types/int-uint.md)) - Port used by the initiator
- `cluster` ([String](../../sql-reference/data-types/string.md)) — Cluster name.
- `query` ([String](../../sql-reference/data-types/string.md)) — Query executed.
- `settings` ([Map(String, String)](../../sql-reference/data-types/map.md)) - Settings used in the DDL operation
- `query_create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query created time.
- `host` ([String](../../sql-reference/data-types/string.md)) — Hostname
- `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — Host Port.
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Status of the query.
- `exception_code` ([Enum8](../../sql-reference/data-types/enum.md)) — Exception code.
- `exception_text` ([Nullable(String)](../../sql-reference/data-types/string.md)) - Exception message
- `query_finish_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query finish time.
- `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Duration of query execution (in milliseconds).


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
entry_version:     5
initiator_host:    clickhouse01
initiator_port:    9000
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
settings:          {'max_threads':'16','use_uncompressed_cache':'0'}
query_create_time: 2023-09-01 16:15:14
host:              clickhouse-01
port:              9000
status:            Finished
exception_code:    0
exception_text:    
query_finish_time: 2023-09-01 16:15:14
query_duration_ms: 154

Row 2:
──────
entry:             query-0000000001
entry_version:     5
initiator_host:    clickhouse01
initiator_port:    9000
cluster:           test_cluster
query:             CREATE DATABASE test_db UUID '4a82697e-c85e-4e5b-a01e-a36f2a758456' ON CLUSTER test_cluster
settings:          {'max_threads':'16','use_uncompressed_cache':'0'}
query_create_time: 2023-09-01 16:15:14
host:              clickhouse-01
port:              9000
status:            Finished
exception_code:    630
exception_text:    Code: 630. DB::Exception: Cannot drop or rename test_db, because some tables depend on it:
query_finish_time: 2023-09-01 16:15:14
query_duration_ms: 154

2 rows in set. Elapsed: 0.025 sec.
```
