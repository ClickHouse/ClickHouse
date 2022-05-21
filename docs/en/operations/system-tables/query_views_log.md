# query_views_log {#system_tables-query_views_log}

Contains information about the dependent views executed when running a query, for example, the view type or the execution time.

To start logging:

1. Configure parameters in the [query_views_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_views_log) section.
2. Set [log_query_views](../../operations/settings/settings.md#settings-log-query-views) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_views_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_views_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

You can use the [log_queries_probability](../../operations/settings/settings.md#log-queries-probability) setting to reduce the number of queries, registered in the `query_views_log` table.

Columns:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — The date when the last event of the view happened.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the view finished execution.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the view finished execution with microseconds precision.
-   `view_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of view execution (sum of its stages) in milliseconds.
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `view_name` ([String](../../sql-reference/data-types/string.md)) — Name of the view.
-   `view_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — UUID of the view.
-   `view_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the view. Values:
    -   `'Default' = 1` — [Default views](../../sql-reference/statements/create/view.md#normal). Should not appear in this log.
    -   `'Materialized' = 2` — [Materialized views](../../sql-reference/statements/create/view.md#materialized).
    -   `'Live' = 3` — [Live views](../../sql-reference/statements/create/view.md#live-view).
-   `view_query` ([String](../../sql-reference/data-types/string.md)) — The query executed by the view.
-   `view_target` ([String](../../sql-reference/data-types/string.md)) — The name of the view target table.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read rows.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read bytes.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of written rows.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of written bytes.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The maximum difference between the amount of allocated and freed memory in context of this view.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — ProfileEvents that measure different metrics. The description of them could be found in the table [system.events](../../operations/system-tables/events.md#system_tables-events).
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Status of the view. Values:
    -   `'QueryStart' = 1` — Successful start the view execution. Should not appear.
    -   `'QueryFinish' = 2` — Successful end of the view execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of the view execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the view execution.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — Code of an exception.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — Exception message.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [Stack trace](https://en.wikipedia.org/wiki/Stack_trace). An empty string, if the query was completed successfully.

**Example**

Query:

``` sql
SELECT * FROM system.query_views_log LIMIT 1 \G;
```

Result:

``` text
Row 1:
──────
event_date:              2021-06-22
event_time:              2021-06-22 13:23:07
event_time_microseconds: 2021-06-22 13:23:07.738221
view_duration_ms:        0
initial_query_id:        c3a1ac02-9cad-479b-af54-9e9c0a7afd70
view_name:               default.matview_inner
view_uuid:               00000000-0000-0000-0000-000000000000
view_type:               Materialized
view_query:              SELECT * FROM default.table_b
view_target:             default.`.inner.matview_inner`
read_rows:               4
read_bytes:              64
written_rows:            2
written_bytes:           32
peak_memory_usage:       4196188
ProfileEvents:           {'FileOpen':2,'WriteBufferFromFileDescriptorWrite':2,'WriteBufferFromFileDescriptorWriteBytes':187,'IOBufferAllocs':3,'IOBufferAllocBytes':3145773,'FunctionExecute':3,'DiskWriteElapsedMicroseconds':13,'InsertedRows':2,'InsertedBytes':16,'SelectedRows':4,'SelectedBytes':48,'ContextLock':16,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':698,'SoftPageFaults':4,'OSReadChars':463}
status:                  QueryFinish
exception_code:          0
exception:
stack_trace:
```

**See Also**

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — Description of the `query_log` system table which contains common information about queries execution.
-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — This table contains information about each query execution thread.

[Original article](https://clickhouse.com/docs/en/operations/system_tables/query_thread_log) <!--hide-->
