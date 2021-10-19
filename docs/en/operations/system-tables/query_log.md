# system.query_log {#system_tables-query_log}

Contains information about executed queries, for example, start time, duration of processing, error messages.

!!! note "Note"
    This table does not contain the ingested data for `INSERT` queries.

You can change settings of queries logging in the [query_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) section of the server configuration.

You can disable queries logging by setting [log_queries = 0](../../operations/settings/settings.md#settings-log-queries). We do not recommend to turn off logging because information in this table is important for solving issues.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

The `system.query_log` table registers two kinds of queries:

1.  Initial queries that were run directly by the client.
2.  Child queries that were initiated by other queries (for distributed query execution). For these types of queries, information about the parent queries is shown in the `initial_*` columns.

Each query creates one or two rows in the `query_log` table, depending on the status (see the `type` column) of the query:

1.  If the query execution was successful, two rows with the `QueryStart` and `QueryFinish` types are created.
2.  If an error occurred during query processing, two events with the `QueryStart` and `ExceptionWhileProcessing` types are created.
3.  If an error occurred before launching the query, a single event with the `ExceptionBeforeStart` type is created.

You can use the [log_queries_probability](../../operations/settings/settings.md#log-queries-probability) setting to reduce the number of queries, registered in the `query_log` table.

Columns:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of an event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Query starting date.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query starting time.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — Query starting time with microseconds precision.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Start time of query execution with microsecond precision.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution in milliseconds.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number of rows read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` and `JOIN`. For distributed queries `read_rows` includes the total number of rows read at all replicas. Each replica sends it’s `read_rows` value, and the server-initiator of the query summarizes all received and local values. The cache volumes do not affect this value.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number of bytes read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` and `JOIN`. For distributed queries `read_bytes` includes the total number of rows read at all replicas. Each replica sends it’s `read_bytes` value, and the server-initiator of the query summarizes all received and local values. The cache volumes do not affect this value.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written rows. For other queries, the column value is 0.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written bytes. For other queries, the column value is 0.
-   `result_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of rows in a result of the `SELECT` query, or a number of rows in the `INSERT` query.
-   `result_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — RAM volume in bytes used to store a query result.
-   `memory_usage` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Memory consumption by the query.
-   `current_database` ([String](../../sql-reference/data-types/string.md)) — Name of the current database.
-   `query` ([String](../../sql-reference/data-types/string.md)) — Query string.
-   `normalized_query_hash` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Identical hash value without the values of literals for similar queries.
-   `query_kind` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — Type of the query.
-   `databases` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — Names of the databases present in the query.
-   `tables` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — Names of the tables present in the query.
-   `views` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — Names of the (materialized or live) views present in the query.
-   `columns` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — Names of the columns present in the query.
-   `projections` ([String](../../sql-reference/data-types/string.md)) — Names of the projections used during the query execution.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — Code of an exception.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — Exception message.
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
-   `initial_query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Initial query starting time (for distributed query execution).
-   `initial_query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Initial query starting time with microseconds precision (for distributed query execution).
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
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — HTTP header `UserAgent` passed in the HTTP query.
-   `http_referer` ([String](../../sql-reference/data-types/string.md)) — HTTP header `Referer` passed in the HTTP query (contains an absolute or partial address of the page making the query).
-   `forwarded_for` ([String](../../sql-reference/data-types/string.md)) — HTTP header `X-Forwarded-For` passed in the HTTP query.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — The `quota key` specified in the [quotas](../../operations/quotas.md) setting (see `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — ProfileEvents that measure different metrics. The description of them could be found in the table [system.events](../../operations/system-tables/events.md#system_tables-events)
-   `Settings` ([Map(String, String)](../../sql-reference/data-types/array.md)) — Settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` parameter to 1.
-   `log_comment` ([String](../../sql-reference/data-types/string.md)) — Log comment. It can be set to arbitrary string no longer than [max_query_size](../../operations/settings/settings.md#settings-max_query_size). An empty string if it is not defined.
-   `thread_ids` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Thread ids that are participating in query execution.
-   `used_aggregate_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `aggregate functions`, which were used during query execution.
-   `used_aggregate_function_combinators` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `aggregate functions combinators`, which were used during query execution.
-   `used_database_engines` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `database engines`, which were used during query execution.
-   `used_data_type_families` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `data type families`, which were used during query execution.
-   `used_dictionaries` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `dictionaries`, which were used during query execution.
-   `used_formats` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `formats`, which were used during query execution.
-   `used_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `functions`, which were used during query execution.
-   `used_storages` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `storages`, which were used during query execution.
-   `used_table_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — Canonical names of `table functions`, which were used during query execution.

**Example**

``` sql
SELECT * FROM system.query_log WHERE type = 'QueryFinish' ORDER BY query_start_time DESC LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
type:                                  QueryFinish
event_date:                            2021-07-28
event_time:                            2021-07-28 13:46:56
event_time_microseconds:               2021-07-28 13:46:56.719791
query_start_time:                      2021-07-28 13:46:56
query_start_time_microseconds:         2021-07-28 13:46:56.704542
query_duration_ms:                     14
read_rows:                             8393
read_bytes:                            374325
written_rows:                          0
written_bytes:                         0
result_rows:                           4201
result_bytes:                          153024
memory_usage:                          4714038
current_database:                      default
query:                                 SELECT DISTINCT arrayJoin(extractAll(name, '[\\w_]{2,}')) AS res FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.table_engines UNION ALL SELECT name FROM system.formats UNION ALL SELECT name FROM system.table_functions UNION ALL SELECT name FROM system.data_type_families UNION ALL SELECT name FROM system.merge_tree_settings UNION ALL SELECT name FROM system.settings UNION ALL SELECT cluster FROM system.clusters UNION ALL SELECT macro FROM system.macros UNION ALL SELECT policy_name FROM system.storage_policies UNION ALL SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate UNION ALL SELECT name FROM system.databases LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.tables LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.dictionaries LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.columns LIMIT 10000) WHERE notEmpty(res)
normalized_query_hash:                 6666026786019643712
query_kind:                            Select
databases:                             ['system']
tables:                                ['system.aggregate_function_combinators','system.clusters','system.columns','system.data_type_families','system.databases','system.dictionaries','system.formats','system.functions','system.macros','system.merge_tree_settings','system.settings','system.storage_policies','system.table_engines','system.table_functions','system.tables']
columns:                               ['system.aggregate_function_combinators.name','system.clusters.cluster','system.columns.name','system.data_type_families.name','system.databases.name','system.dictionaries.name','system.formats.name','system.functions.is_aggregate','system.functions.name','system.macros.macro','system.merge_tree_settings.name','system.settings.name','system.storage_policies.policy_name','system.table_engines.name','system.table_functions.name','system.tables.name']
projections:                           []
exception_code:                        0
exception:
stack_trace:
is_initial_query:                      1
user:                                  default
query_id:                              a3361f6e-a1fd-4d54-9f6f-f93a08bab0bf
address:                               ::ffff:127.0.0.1
port:                                  51006
initial_user:                          default
initial_query_id:                      a3361f6e-a1fd-4d54-9f6f-f93a08bab0bf
initial_address:                       ::ffff:127.0.0.1
initial_port:                          51006
initial_query_start_time:              2021-07-28 13:46:56
initial_query_start_time_microseconds: 2021-07-28 13:46:56.704542
interface:                             1
os_user:
client_hostname:
client_name:                           ClickHouse client
client_revision:                       54449
client_version_major:                  21
client_version_minor:                  8
client_version_patch:                  0
http_method:                           0
http_user_agent:
http_referer:
forwarded_for:
quota_key:
revision:                              54453
log_comment:
thread_ids:                            [5058,22097,22110,22094]
ProfileEvents.Names:                   ['Query','SelectQuery','ArenaAllocChunks','ArenaAllocBytes','FunctionExecute','NetworkSendElapsedMicroseconds','SelectedRows','SelectedBytes','ContextLock','RWLockAcquiredReadLocks','RealTimeMicroseconds','UserTimeMicroseconds','SystemTimeMicroseconds','SoftPageFaults','OSCPUWaitMicroseconds','OSCPUVirtualTimeMicroseconds','OSWriteBytes','OSWriteChars']
ProfileEvents.Values:                  [1,1,39,352256,64,360,8393,374325,412,440,34480,13108,4723,671,19,17828,8192,10240]
Settings.Names:                        ['load_balancing','max_memory_usage']
Settings.Values:                       ['random','10000000000']
used_aggregate_functions:              []
used_aggregate_function_combinators:   []
used_database_engines:                 []
used_data_type_families:               ['UInt64','UInt8','Nullable','String','date']
used_dictionaries:                     []
used_formats:                          []
used_functions:                        ['concat','notEmpty','extractAll']
used_storages:                         []
used_table_functions:                  []
```

**See Also**

-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — This table contains information about each query execution thread.
-   [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) — This table contains information about each view executed during a query.

[Original article](https://clickhouse.com/docs/en/operations/system-tables/query_log) <!--hide-->
