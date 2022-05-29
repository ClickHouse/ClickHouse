# processes {#system_tables-processes}

This system table is used for implementing the `SHOW PROCESSLIST` query.

Columns:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` user. The field contains the username for a specific query, not for a query that this query initiated.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` on the query requestor server.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage) setting.
-   `query` (String) – The query text. For `INSERT`, it does not include the data to insert.
-   `query_id` (String) – Query ID, if defined.
-   `is_cancelled` (Int8) – Was query cancelled.
-   `is_all_data_sent` (Int8) – Was all data sent to the client (in other words query had been finished on the server).

```sql
:) SELECT * FROM system.processes LIMIT 10 FORMAT Vertical;
```

```text
Row 1:
──────
is_initial_query:     1
user:                 default
query_id:             35a360fa-3743-441d-8e1f-228c938268da
address:              ::ffff:172.23.0.1
port:                 47588
initial_user:         default
initial_query_id:     35a360fa-3743-441d-8e1f-228c938268da
initial_address:      ::ffff:172.23.0.1
initial_port:         47588
interface:            1
os_user:              bharatnc
client_hostname:      tower
client_name:          ClickHouse
client_revision:      54437
client_version_major: 20
client_version_minor: 7
client_version_patch: 2
http_method:          0
http_user_agent:
quota_key:
elapsed:              0.000582537
is_cancelled:         0
is_all_data_sent:     0
read_rows:            0
read_bytes:           0
total_rows_approx:    0
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
query:                SELECT * from system.processes LIMIT 10 FORMAT Vertical;
thread_ids:           [67]
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
Settings:             {'background_pool_size':'32','load_balancing':'random','allow_suspicious_low_cardinality_types':'1','distributed_aggregation_memory_efficient':'1','skip_unavailable_shards':'1','log_queries':'1','max_bytes_before_external_group_by':'20000000000','max_bytes_before_external_sort':'20000000000','allow_introspection_functions':'1'}

1 rows in set. Elapsed: 0.002 sec.
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/processes) <!--hide-->
