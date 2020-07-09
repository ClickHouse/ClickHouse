# system.query\_log {#system_tables-query_log}

Contains information about executed queries, for example, start time, duration of processing, error messages.

!!! note "Note"
    This table doesn’t contain the ingested data for `INSERT` queries.

You can change settings of queries logging in the [query\_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) section of the server configuration.

You can disable queries logging by setting [log\_queries = 0](../../operations/settings/settings.md#settings-log-queries). We don’t recommend to turn off logging because information in this table is important for solving issues.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query\_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse doesn’t delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

The `system.query_log` table registers two kinds of queries:

1.  Initial queries that were run directly by the client.
2.  Child queries that were initiated by other queries (for distributed query execution). For these types of queries, information about the parent queries is shown in the `initial_*` columns.

Each query creates one or two rows in the `query_log` table, depending on the status (see the `type` column) of the query:

1.  If the query execution was successful, two rows with the `QueryStart` and `QueryFinish` types are created .
2.  If an error occurred during query processing, two events with the `QueryStart` and `ExceptionWhileProcessing` types are created .
3.  If an error occurred before launching the query, a single event with the `ExceptionBeforeStart` type is created.

Columns:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of an event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Query starting date.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query starting time.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution in milliseconds.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number or rows read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` and `JOIN`. For distributed queries `read_rows` includes the total number of rows read at all replicas. Each replica sends it’s `read_rows` value, and the server-initiator of the query summarize all received and local values. The cache volumes doesn’t affect this value.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number or bytes read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` and `JOIN`. For distributed queries `read_bytes` includes the total number of rows read at all replicas. Each replica sends it’s `read_bytes` value, and the server-initiator of the query summarize all received and local values. The cache volumes doesn’t affect this value.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written rows. For other queries, the column value is 0.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written bytes. For other queries, the column value is 0.
-   `result_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of rows in a result of the `SELECT` query, or a number of rows in the `INSERT` query.
-   `result_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — RAM volume in bytes used to store a query result.
-   `memory_usage` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Memory consumption by the query.
-   `query` ([String](../../sql-reference/data-types/string.md)) — Query string.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — Exception message.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — Code of an exception.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [Stack trace](https://en.wikipedia.org/wiki/Stack_trace). An empty string, if the query was completed successfully.
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query as part of distributed query execution.
-   `user` ([String](../../sql-reference/data-types/string.md)) — Name of the user who initiated the current query.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the query.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The client port that was used to make the query.
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that the parent query was launched from.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The client port that was used to make the parent query.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — Operating system username who runs [clickhouse-client](../../interfaces/cli.md).
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — Hostname of the client machine where the [clickhouse-client](../../interfaces/cli.md) or another TCP client is run.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — The [clickhouse-client](../../interfaces/cli.md) or another TCP client name.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Revision of the [clickhouse-client](../../interfaces/cli.md) or another TCP client.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Major version of the [clickhouse-client](../../interfaces/cli.md) or another TCP client.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Minor version of the [clickhouse-client](../../interfaces/cli.md) or another TCP client.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Patch component of the [clickhouse-client](../../interfaces/cli.md) or another TCP client version.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` method was used.
    -   2 — `POST` method was used.
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — The `UserAgent` header passed in the HTTP request.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — The “quota key” specified in the [quotas](../../operations/quotas.md) setting (see `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `thread_numbers` ([Array(UInt32)](../../sql-reference/data-types/array.md)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` ([Array(String)](../../sql-reference/data-types/array.md)) — Counters that measure different metrics. The description of them could be found in the table [system.events](../../operations/system-tables/events.md#system_tables-events)
-   `ProfileEvents.Values` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Values of metrics that are listed in the `ProfileEvents.Names` column.
-   `Settings.Names` ([Array(String)](../../sql-reference/data-types/array.md)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` parameter to 1.
-   `Settings.Values` ([Array(String)](../../sql-reference/data-types/array.md)) — Values of settings that are listed in the `Settings.Names` column.

**Example**

``` sql
SELECT * FROM system.query_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
type:                 QueryStart
event_date:           2020-05-13
event_time:           2020-05-13 14:02:28
query_start_time:     2020-05-13 14:02:28
query_duration_ms:    0
read_rows:            0
read_bytes:           0
written_rows:         0
written_bytes:        0
result_rows:          0
result_bytes:         0
memory_usage:         0
query:                SELECT 1
exception_code:       0
exception:
stack_trace:
is_initial_query:     1
user:                 default
query_id:             5e834082-6f6d-4e34-b47b-cd1934f4002a
address:              ::ffff:127.0.0.1
port:                 57720
initial_user:         default
initial_query_id:     5e834082-6f6d-4e34-b47b-cd1934f4002a
initial_address:      ::ffff:127.0.0.1
initial_port:         57720
interface:            1
os_user:              bayonet
client_hostname:      clickhouse.ru-central1.internal
client_name:          ClickHouse client
client_revision:      54434
client_version_major: 20
client_version_minor: 4
client_version_patch: 1
http_method:          0
http_user_agent:
quota_key:
revision:             54434
thread_ids:           []
ProfileEvents.Names:  []
ProfileEvents.Values: []
Settings.Names:       ['use_uncompressed_cache','load_balancing','log_queries','max_memory_usage']
Settings.Values:      ['0','random','1','10000000000']
```

**See Also**

-   [system.query\_thread\_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — This table contains information about each query execution thread.
