# system.query_thread_log {#system_tables-query_thread_log}

Contains information about threads that execute queries, for example, thread name, thread start time, duration of query processing.

To start logging:

1.  Configure parameters in the [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) section.
2.  Set [log_query_threads](../../operations/settings/settings.md#settings-log-query-threads) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

Columns:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — The date when the thread has finished execution of the query.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the thread has finished execution of the query.
-   `event_time_microsecinds` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the thread has finished execution of the query with microseconds precision.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Start time of query execution with microsecond precision.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read rows.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read bytes.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written rows. For other queries, the column value is 0.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written bytes. For other queries, the column value is 0.
-   `memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` ([String](../../sql-reference/data-types/string.md)) — Name of the thread.
-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Internal thread ID.
-   `thread_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — thread ID.
-   `master_thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — OS initial ID of initial thread.
-   `query` ([String](../../sql-reference/data-types/string.md)) — Query string.
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` ([String](../../sql-reference/data-types/string.md)) — Name of the user who initiated the current query.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the query.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — The client port that was used to make the query.
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that the parent query was launched from.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — The client port that was used to make the parent query.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — OS’s username who runs [clickhouse-client](../../interfaces/cli.md).
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — Hostname of the client machine where the [clickhouse-client](../../interfaces/cli.md) or another TCP client is run.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — The [clickhouse-client](../../interfaces/cli.md) or another TCP client name.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Revision of the [clickhouse-client](../../interfaces/cli.md) or another TCP client.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Major version of the [clickhouse-client](../../interfaces/cli.md) or another TCP client.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Minor version of the [clickhouse-client](../../interfaces/cli.md) or another TCP client.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Patch component of the [clickhouse-client](../../interfaces/cli.md) or another TCP client version.
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` method was used.
    -   2 — `POST` method was used.
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — The `UserAgent` header passed in the HTTP request.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — The “quota key” specified in the [quotas](../../operations/quotas.md) setting (see `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — ProfileEvents that measure different metrics for this thread. The description of them could be found in the table [system.events](#system_tables-events).

**Example**

``` sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.134042
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063150
query_duration_ms:             70
read_rows:                     0
read_bytes:                    0
written_rows:                  1
written_bytes:                 12
memory_usage:                  4300844
peak_memory_usage:             4300844
thread_name:                   TCPHandler
thread_id:                     638133
master_thread_id:              638133
query:                         INSERT INTO test1 VALUES
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:
quota_key:
revision:                      54440
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
```

**See Also**

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — Description of the `query_log` system table which contains common information about queries execution.

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/query_thread_log) <!--hide-->
