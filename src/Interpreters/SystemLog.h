#pragma once

#include "config.h"

#include <Interpreters/StorageID.h>
#include <Common/SystemLogBase.h>
#include <Common/Exception.h>
#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/CommonParsers.h>

#include <Interpreters/SystemLogFlushPolicy.h>
#include <boost/noncopyable.hpp>

#define LIST_OF_ALL_SYSTEM_LOGS(M) \
    M(QueryLog,              query_log,            DB::SYSTEM_LOG_DOCUMENTATION_QUERY_LOG) \
    M(QueryThreadLog,        query_thread_log,     DB::SYSTEM_LOG_DOCUMENTATION_QUERY_THREAD_LOG) \
    M(PartLog,               part_log,             DB::SYSTEM_LOG_DOCUMENTATION_PART_LOG) \
    M(BackgroundSchedulePoolLog, background_schedule_pool_log, DB::SYSTEM_LOG_DOCUMENTATION_BACKGROUND_SCHEDULE_POOL_LOG) \
    M(TraceLog,              trace_log,            DB::SYSTEM_LOG_DOCUMENTATION_TRACE_LOG) \
    M(CrashLog,              crash_log,            DB::SYSTEM_LOG_DOCUMENTATION_CRASH_LOG) \
    M(TextLog,               text_log,             DB::SYSTEM_LOG_DOCUMENTATION_TEXT_LOG) \
    M(MetricLog,             metric_log,           DB::SYSTEM_LOG_DOCUMENTATION_METRIC_LOG) \
    M(TransposedMetricLog,   transposed_metric_log,"Contains history of metrics values from tables system.metrics and system.events. Periodically flushed to disk. Transposed form of system.metric_log.") \
    M(BucketedMetricLog,     bucketed_metric_log,  "Contains history of metrics values from tables system.metrics and system.events. Periodically flushed to disk. Single Map column form of system.metric_log with bucketed serialization.") \
    M(ErrorLog,              error_log,            DB::SYSTEM_LOG_DOCUMENTATION_ERROR_LOG) \
    M(FilesystemCacheLog,    filesystem_cache_log, DB::SYSTEM_LOG_DOCUMENTATION_FILESYSTEM_CACHE_LOG) \
    M(FilesystemReadPrefetchesLog, filesystem_read_prefetches_log, DB::SYSTEM_LOG_DOCUMENTATION_FILESYSTEM_READ_PREFETCHES_LOG) \
    M(ObjectStorageQueueLog, s3queue_log,          DB::SYSTEM_LOG_DOCUMENTATION_S3QUEUE_LOG) \
    M(ObjectStorageQueueLog, azure_queue_log,      DB::SYSTEM_LOG_DOCUMENTATION_AZURE_QUEUE_LOG) \
    M(AsynchronousMetricLog, asynchronous_metric_log, DB::SYSTEM_LOG_DOCUMENTATION_ASYNCHRONOUS_METRIC_LOG) \
    M(OpenTelemetrySpanLog,  opentelemetry_span_log, DB::SYSTEM_LOG_DOCUMENTATION_OPENTELEMETRY_SPAN_LOG) \
    M(QueryViewsLog,         query_views_log,      DB::SYSTEM_LOG_DOCUMENTATION_QUERY_VIEWS_LOG) \
    M(ZooKeeperLog,          zookeeper_log,        DB::SYSTEM_LOG_DOCUMENTATION_ZOOKEEPER_LOG) \
    M(SessionLog,            session_log,          DB::SYSTEM_LOG_DOCUMENTATION_SESSION_LOG) \
    M(TransactionsInfoLog,   transactions_info_log, DB::SYSTEM_LOG_DOCUMENTATION_TRANSACTIONS_INFO_LOG) \
    M(ProcessorsProfileLog,  processors_profile_log, DB::SYSTEM_LOG_DOCUMENTATION_PROCESSORS_PROFILE_LOG) \
    M(AsynchronousInsertLog, asynchronous_insert_log, DB::SYSTEM_LOG_DOCUMENTATION_ASYNCHRONOUS_INSERT_LOG) \
    M(BackupLog,             backup_log,           DB::SYSTEM_LOG_DOCUMENTATION_BACKUP_LOG) \
    M(BlobStorageLog,        blob_storage_log,     DB::SYSTEM_LOG_DOCUMENTATION_BLOB_STORAGE_LOG) \
    M(QueryMetricLog,        query_metric_log,     DB::SYSTEM_LOG_DOCUMENTATION_QUERY_METRIC_LOG) \
    M(DeadLetterQueue,       dead_letter_queue,    DB::SYSTEM_LOG_DOCUMENTATION_DEAD_LETTER_QUEUE) \
    M(ZooKeeperConnectionLog, zookeeper_connection_log, DB::SYSTEM_LOG_DOCUMENTATION_ZOOKEEPER_CONNECTION_LOG) \
    M(AggregatedZooKeeperLog, aggregated_zookeeper_log, DB::SYSTEM_LOG_DOCUMENTATION_AGGREGATED_ZOOKEEPER_LOG) \
    M(IcebergMetadataLog,    iceberg_metadata_log, DB::SYSTEM_LOG_DOCUMENTATION_ICEBERG_METADATA_LOG) \
    M(DeltaMetadataLog,    delta_lake_metadata_log, DB::SYSTEM_LOG_DOCUMENTATION_DELTA_LAKE_METADATA_LOG) \
    M(PredicateStatisticsLog, predicate_statistics_log, DB::SYSTEM_LOG_DOCUMENTATION_PREDICATE_STATISTICS_LOG) \

#define LIST_OF_CLOUD_SYSTEM_LOGS(M) \
    M(DistributedCacheLog, distributed_cache_log, "Contains the history of all interactions with distributed cache.") \
    M(DistributedCacheServerLog, distributed_cache_server_log, "Contains the history of all interactions with distributed cache client.") \


namespace DB
{

inline constexpr char SYSTEM_LOG_DOCUMENTATION_QUERY_LOG[] = R"DOCS_MD(
.description
Stores metadata and statistics about executed queries, such as start time, duration, error messages, resource usage, and other execution details. It does not store the results of queries.

You can change settings of queries logging in the [query_log](/reference/settings/server-settings/settings/query#query_log) section of the server configuration.

You can disable queries logging by setting [log_queries = 0](/reference/settings/session-settings/log-queries#log_queries). We do not recommend to turn off logging because information in this table is important for solving issues.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_log](/reference/settings/server-settings/settings/query#query_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

The `system.query_log` table registers two kinds of queries:

1.  Initial (top-level) queries.
2.  Child queries that were initiated by other queries, including queries for distributed execution and internal subqueries such as view evaluation. For these queries, information about the original initial query is shown in the `initial_*` columns.

<Tip>
**Filter initial queries by default**

Generally, add `is_initial_query = 1` whenever you query `system.query_log`. This excludes child queries so individual processing steps are not counted separately from the initial query. This filter does not imply that a query was submitted by a client, because server-internal work can also be an initial query.

Use `initial_query_id` instead when you need to trace an initial query together with child queries that preserve its ID. The initial query has the same value for `initial_query_id` and `query_id`, while child queries in the same chain keep the initial query's `initial_query_id` and have their own `query_id`. Not all work spawned by an initial query is correlated this way: server-dispatched work can start a new initial-query chain with a new `initial_query_id`, as with remote [`QueryRunner`](/reference/engines/table-engines/special/query-runner#cluster-mode) dispatches.

```sql
SELECT
    hostname,
    type,
    query_id,
    initial_query_id,
    is_initial_query,
    query
FROM system.query_log
WHERE initial_query_id = '<query_id_of_initial_query>'
ORDER BY event_time_microseconds;
```

If correlated child queries can run on other nodes, query `system.query_log` on every node, for example with [`clusterAllReplicas`](/reference/functions/table-functions/cluster).
</Tip>

Each query creates one or two rows in the `query_log` table, depending on the status (see the `type` column) of the query:

1.  If the query execution was successful, two rows with the `QueryStart` and `QueryFinish` types are created.
2.  If an error occurred during query processing, two events with the `QueryStart` and `ExceptionWhileProcessing` types are created.
3.  If an error occurred before launching the query, a single event with the `ExceptionBeforeStart` type is created.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability) setting to reduce the number of queries, registered in the `query_log` table.

You can use the [log_formatted_queries](/reference/settings/session-settings/log#log_formatted_queries) setting to log formatted queries to the `formatted_query` column.

.examples
**Basic example**

```sql
SELECT *
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
ORDER BY query_start_time DESC
LIMIT 1
FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                              clickhouse.eu-central1.internal
type:                                  QueryFinish
event_date:                            2021-11-03
event_time:                            2021-11-03 16:13:54
event_time_microseconds:               2021-11-03 16:13:54.953024
query_start_time:                      2021-11-03 16:13:54
query_start_time_microseconds:         2021-11-03 16:13:54.952325
query_duration_ms:                     0
read_rows:                             69
read_bytes:                            6187
written_rows:                          0
written_bytes:                         0
result_rows:                           69
result_bytes:                          48256
memory_usage:                          0
current_database:                      default
query:                                 DESCRIBE TABLE system.query_log
formatted_query:
normalized_query_hash:                 8274064835331539124
query_kind:
databases:                             []
tables:                                []
columns:                               []
projections:                           []
views:                                 []
exception_code:                        0
exception:
stack_trace:
is_initial_query:                      1
user:                                  default
query_id:                              7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
address:                               ::ffff:127.0.0.1
port:                                  40452
initial_user:                          default
initial_query_id:                      7c28bbbb-753b-4eba-98b1-efcbe2b9bdf6
initial_address:                       ::ffff:127.0.0.1
initial_port:                          40452
initial_query_start_time:              2021-11-03 16:13:54
initial_query_start_time_microseconds: 2021-11-03 16:13:54.952325
interface:                             1
os_user:                               sevirov
client_hostname:                       clickhouse.eu-central1.internal
client_name:                           ClickHouse
client_revision:                       54449
client_version_major:                  21
client_version_minor:                  10
client_version_patch:                  1
http_method:                           0
http_user_agent:
http_referer:
forwarded_for:
quota_key:
revision:                              54456
log_comment:
thread_ids:                            [30776,31174]
ProfileEvents:                         {'Query':1,'NetworkSendElapsedMicroseconds':59,'NetworkSendBytes':2643,'SelectedRows':69,'SelectedBytes':6187,'ContextLock':9,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':817,'UserTimeMicroseconds':427,'SystemTimeMicroseconds':212,'OSCPUVirtualTimeMicroseconds':639,'OSReadChars':894,'OSWriteChars':319}
Settings:                              {'load_balancing':'random','max_memory_usage':'10000000000'}
used_aggregate_functions:              []
used_aggregate_function_combinators:   []
used_database_engines:                 []
used_data_type_families:               []
used_dictionaries:                     []
used_formats:                          []
used_functions:                        []
used_storages:                         []
used_table_functions:                  []
used_executable_user_defined_functions:[]
used_sql_user_defined_functions:       []
used_privileges:                       []
missing_privileges:                    []
query_cache_usage:                     None
```

**Cloud example**

In ClickHouse Cloud, `system.query_log` is local to each node; to see all entries you must query via [`clusterAllReplicas`](/reference/functions/table-functions/cluster).

For example, to aggregate query_log rows from every replica in the “default” cluster you can write:

```sql
SELECT *
FROM clusterAllReplicas('default', system.query_log)
WHERE event_time >= now() - toIntervalHour(1)
LIMIT 10
SETTINGS skip_unavailable_shards = 1;
```

.see_also
- [system.query_thread_log](/reference/system-tables/query_thread_log) — This table contains information about each query execution thread.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_QUERY_THREAD_LOG[] = R"DOCS_MD(
.description
Contains information about threads that execute queries, for example, thread name, thread start time, duration of query processing.

To start logging:

1.  Configure parameters in the [query_thread_log](/reference/settings/server-settings/settings/query#query_thread_log) section.
2.  Set [log_query_threads](/reference/settings/session-settings/log-query#log_query_threads) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_thread_log](/reference/settings/server-settings/settings/query#query_thread_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability)) setting to reduce the number of queries, registered in the `query_thread_log` table.

.examples
```sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                      clickhouse.eu-central1.internal
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

.see_also
- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.query_views_log](/reference/system-tables/query_views_log) — This table contains information about each view executed during a query.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_PART_LOG[] = R"DOCS_MD(
.description
The `system.part_log` table is created only if the [part_log](/reference/settings/server-settings/settings/other#part_log) server setting is specified.

This table contains information about events that occurred with [data parts](/reference/engines/table-engines/mergetree-family/custom-partitioning-key) in the [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) family tables, such as adding or merging data.

The `system.part_log` table contains the following columns:

.columns_notes
The `system.part_log` table is created after the first inserting data to the `MergeTree` table.

.examples
```sql
SELECT * FROM system.part_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
query_id:
event_type:              MergeParts
merge_reason:            RegularMerge
merge_algorithm:         Vertical
event_date:              2025-07-19
event_time:              2025-07-19 23:54:19
event_time_microseconds: 2025-07-19 23:54:19.710761
duration_ms:             2158
database:                default
table:                   github_events
table_uuid:              1ad33424-f5f5-402b-ac03-ec82282634ab
part_name:               all_1_7_1
partition_id:            all
partition:               tuple()
part_type:               Wide
part_storage_type:       Full
disk_name:               default
path_on_disk:            ./data/store/1ad/1ad33424-f5f5-402b-ac03-ec82282634ab/all_1_7_1/
rows:                    3285726 -- 3.29 million
size_in_bytes:           438968542 -- 438.97 million
merged_from:             ['all_1_1_0','all_2_2_0','all_3_3_0','all_4_4_0','all_5_5_0','all_6_6_0','all_7_7_0']
bytes_uncompressed:      1373137767 -- 1.37 billion
read_rows:               3285726 -- 3.29 million
read_bytes:              1429206946 -- 1.43 billion
peak_memory_usage:       303611887 -- 303.61 million
error:                   0
exception:
mutation_ids:
ProfileEvents:           {'FileOpen':703,'ReadBufferFromFileDescriptorRead':3824,'ReadBufferFromFileDescriptorReadBytes':439601681,'WriteBufferFromFileDescriptorWrite':592,'WriteBufferFromFileDescriptorWriteBytes':438988500,'ReadCompressedBytes':439601681,'CompressedReadBufferBlocks':6314,'CompressedReadBufferBytes':1539835748,'OpenedFileCacheHits':50,'OpenedFileCacheMisses':484,'OpenedFileCacheMicroseconds':222,'IOBufferAllocs':1914,'IOBufferAllocBytes':319810140,'ArenaAllocChunks':8,'ArenaAllocBytes':131072,'MarkCacheMisses':7,'CreatedReadBufferOrdinary':534,'DiskReadElapsedMicroseconds':139058,'DiskWriteElapsedMicroseconds':51639,'AnalyzePatchRangesMicroseconds':28,'ExternalProcessingFilesTotal':1,'RowsReadByMainReader':170857759,'WaitMarksLoadMicroseconds':988,'LoadedMarksFiles':7,'LoadedMarksCount':14,'LoadedMarksMemoryBytes':728,'Merge':2,'MergeSourceParts':14,'MergedRows':3285733,'MergedColumns':4,'GatheredColumns':51,'MergedUncompressedBytes':1429207058,'MergeTotalMilliseconds':2158,'MergeExecuteMilliseconds':2155,'MergeHorizontalStageTotalMilliseconds':145,'MergeHorizontalStageExecuteMilliseconds':145,'MergeVerticalStageTotalMilliseconds':2008,'MergeVerticalStageExecuteMilliseconds':2006,'MergeProjectionStageTotalMilliseconds':5,'MergeProjectionStageExecuteMilliseconds':4,'MergingSortedMilliseconds':7,'GatheringColumnMilliseconds':56,'ContextLock':2091,'PartsLockHoldMicroseconds':77,'PartsLockWaitMicroseconds':1,'RealTimeMicroseconds':2157475,'CannotWriteToWriteBufferDiscard':36,'LogTrace':6,'LogDebug':59,'LoggerElapsedNanoseconds':514040,'ConcurrencyControlSlotsGranted':53,'ConcurrencyControlSlotsAcquired':53}
```
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_BACKGROUND_SCHEDULE_POOL_LOG[] = R"DOCS_MD(
.description
The `system.background_schedule_pool_log` table is created only if the [background_schedule_pool_log](/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_log) server setting is specified.

This table contains the history of background schedule pool task executions. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, and message broker operations.

.columns_notes
The `system.background_schedule_pool_log` table is created after the first background task execution.

.examples
```sql
SELECT * FROM system.background_schedule_pool_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-12-18
event_time:              2025-12-18 10:30:15
event_time_microseconds: 2025-12-18 10:30:15.123456
query_id:
database:                default
table:                   data
table_uuid:              00000000-0000-0000-0000-000000000000
log_name:                default.data
duration_ms:             42
error:                   0
exception:
```

.see_also
- [system.background_schedule_pool](/reference/system-tables/background_schedule_pool) — Contains information about currently scheduled tasks in background schedule pools.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_TRACE_LOG[] = R"DOCS_MD(
.description
Contains stack traces collected by the [sampling query profiler](/concepts/features/performance/troubleshoot/sampling-query-profiler).

ClickHouse creates this table when the [trace_log](/reference/settings/server-settings/settings/other#trace_log) server configuration section is set. Also see settings: [query_profiler_real_time_period_ns](/reference/settings/session-settings/query-profiler#query_profiler_real_time_period_ns), [query_profiler_cpu_time_period_ns](/reference/settings/session-settings/query-profiler#query_profiler_cpu_time_period_ns), [memory_profiler_step](/reference/settings/session-settings/memory-profiler#memory_profiler_step),
[memory_profiler_sample_probability](/reference/settings/session-settings/memory-profiler#memory_profiler_sample_probability), [trace_profile_events](/reference/settings/session-settings/trace-profile-events#trace_profile_events).

When symbolization is enabled (the default), the demangled function names and source locations are already available in the `symbols` and `lines` columns, so you can analyze the logs directly without introspection functions. The `symbolize` setting applies to profiler-collected trace types; rows with the `Instrumentation` trace type are symbolized regardless of it. Symbolization is supported on ELF platforms (such as Linux) and macOS; on FreeBSD the `symbols` and `lines` columns are always empty. Function names in `symbols` come from the binary's symbol table and are available by default, while source locations in `lines` are best-effort: they require debug info (a `.dSYM` bundle on macOS) and, on ELF platforms, are resolved only for frames inside the main ClickHouse binary; unresolved frames have empty `lines` entries.
If symbolization is disabled, or you want to resolve the raw addresses in the `trace` column on the fly (for example, to expand inline frames), use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` introspection functions. These functions are available on the same platforms as symbolization (ELF platforms such as Linux, and macOS); on FreeBSD they are not compiled in either, so the addresses in `trace` have to be resolved outside the server.

## Converting to Chrome Event Trace Format {#chrome-event-trace-format}

The profiling data can be converted to Chrome's Event Trace Format with the following query. Save the query to a `chrome_trace.sql` file:

```sql
WITH traces AS (
    SELECT * FROM system.trace_log
    WHERE event_date >= today() AND trace_type = 'Instrumentation' AND handler = 'profile'
    ORDER BY event_time, entry_type
)
SELECT
    format(
        '{{"traceEvents": [{}\n]}}',
        arrayStringConcat(
            groupArray(
                format(
                    '\n{{"name": "{}", "cat": "clickhouse", "ph": "{}", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}", "cpu_id": {}, "stack": [{}]}}}},',
                    function_name,
                    if(entry_type = 0, 'B', 'E'),
                    timestamp_ns/1000,
                    toString(thread_id),
                    query_id,
                    cpu_id,
                    arrayStringConcat(arrayMap((x, y) -> concat('"', x, ': ', y, '", '), lines, symbols))
                )
            )
        )
    )
FROM traces;
```

And executing it with ClickHouse Client to export it to a `trace.json` file that we can import either with [Perfetto](https://ui.perfetto.dev/) or [speedscope](https://www.speedscope.app/).

```bash
echo $(clickhouse client --query "$(cat chrome_trace.sql)") > trace.json
```

We can omit the stack part if we want a more compact but less informative trace.

.columns_notes
Symbolization can be enabled or disabled with the `symbolize` setting under `trace_log` in the server's configuration file. It is enabled by default. The setting applies to profiler-collected trace types; rows with the `Instrumentation` trace type are symbolized regardless of it.

.examples
```sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-11
event_time:              2025-11-11 11:53:59
event_time_microseconds: 2025-11-11 11:53:59.128333
timestamp_ns:            1762862039128333000
revision:                54504
trace_type:              Instrumentation
cpu_id:                  19
thread_id:               3166432 -- 3.17 million
query_id:                ef462508-e189-4ea2-b231-4489506728e8
trace:                   [350594916,447733712,447742095,447727324,447726659,221642873,450882315,451852359,451905441,451885554,512404306,512509092,612861767,612863269,612466367,612455825,137631896259267,137631896856768]
size:                    0
ptr:                     0
memory_context:          Unknown
memory_blocked_context:  Unknown
event:
increment:               0
symbols:                 ['StackTrace::StackTrace()','DB::InstrumentationManager::createTraceLogElement(DB::InstrumentationManager::InstrumentedPointInfo const&, XRayEntryType, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>) const','DB::InstrumentationManager::profile(XRayEntryType, DB::InstrumentationManager::InstrumentedPointInfo const&)','DB::InstrumentationManager::dispatchHandlerImpl(int, XRayEntryType)','DB::InstrumentationManager::dispatchHandler(int, XRayEntryType)','__xray_FunctionEntry','DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)','DB::logQueryStart(std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>> const&, std::__1::shared_ptr<DB::Context> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, unsigned long, std::__1::shared_ptr<DB::IAST> const&, DB::QueryPipeline const&, DB::IInterpreter const*, bool, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, bool)','DB::executeQueryImpl(char const*, char const*, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum, std::__1::unique_ptr<DB::ReadBuffer, std::__1::default_delete<DB::ReadBuffer>>&, std::__1::shared_ptr<DB::IAST>&, std::__1::shared_ptr<DB::ImplicitTransactionControlExecutor>, std::__1::function<void ()>)','DB::executeQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum)','DB::TCPHandler::runImpl()','DB::TCPHandler::run()','Poco::Net::TCPServerConnection::start()','Poco::Net::TCPServerDispatcher::run()','Poco::PooledThread::run()','Poco::ThreadImpl::runnableEntry(void*)','start_thread','__clone3']
lines:                   ['./build/../src/Common/StackTrace.cpp:395','./src/Common/StackTrace.h:62','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:738','./build/./src/Interpreters/InstrumentationManager.cpp:257','./build/./src/Interpreters/InstrumentationManager.cpp:225','','./build/./src/Interpreters/QueryMetricLog.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:667','./build/./src/Interpreters/executeQuery.cpp:0','./build/./src/Interpreters/executeQuery.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:744','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:583','./build/../base/poco/Net/src/TCPServerConnection.cpp:54','../contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:80','./build/../base/poco/Foundation/src/ThreadPool.cpp:219','../base/poco/Foundation/include/Poco/AutoPtr.h:77','','']
function_id:             231255
function_name:           DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
handler:                 profile
entry_type:              Exit
duration_nanoseconds:   58435
```

.see_also
- [SYSTEM INSTRUMENT](/reference/statements/system#instrument) — Add or remove instrumentation points.
- [system.instrumentation](/reference/system-tables/instrumentation) — Inspect instrumented points.
- [system.symbols](/reference/system-tables/symbols) — Inspect symbols to add instrumentation points.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_CRASH_LOG[] = R"DOCS_MD(
.description
Contains information about stack traces for fatal errors. The table does not exist in the database by default, it is created only when fatal errors occur.

.examples
```sql title="Query"
SELECT * FROM system.crash_log ORDER BY event_time DESC LIMIT 1;
```

```text title="Response"
Row 1:
──────
hostname:     clickhouse.eu-central1.internal
event_date:   2020-10-14
event_time:   2020-10-14 15:47:40
timestamp_ns: 1602679660271312710
signal:       11
thread_id:    23624
query_id:     428aab7c-8f5c-44e9-9607-d16b44467e69
trace:        [188531193,...]
trace_full:   ['3. DB::(anonymous namespace)::FunctionFormatReadableTimeDelta::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> >&, std::__1::vector<unsigned long, std::__1::allocator<unsigned long> > const&, unsigned long, unsigned long) const @ 0xb3cc1f9 in /home/username/work/ClickHouse/build/programs/clickhouse',...]
version:      ClickHouse 20.11.1.1
revision:     54442
build_id:
```

.see_also
- [trace_log](/reference/system-tables/trace_log) system table
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_TEXT_LOG[] = R"DOCS_MD(
.description
Contains logging entries. The logging level which goes to this table can be limited to the `text_log.level` server setting.

.examples
```sql
SELECT * FROM system.text_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2020-09-10
event_time:              2020-09-10 11:23:07
event_time_microseconds: 2020-09-10 11:23:07.871397
thread_name:             clickhouse-serv
thread_id:               564917
level:                   Information
query_id:
logger_name:             DNSCacheUpdater
message:                 Update period 15 seconds
revision:                54440
source_file:             /ClickHouse/src/Interpreters/DNSCacheUpdater.cpp; void DB::DNSCacheUpdater::start()
source_line:             45
message_format_string:   Update period {} seconds
value1:                  15
value2:
value3:
value4:
value5:
value6:
value7:
value8:
value9:
value10:
```
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_METRIC_LOG[] = R"DOCS_MD(
.description
Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.

.examples
```sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                                                        clickhouse.eu-central1.internal
event_date:                                                      2020-09-05
event_time:                                                      2020-09-05 16:22:33
event_time_microseconds:                                         2020-09-05 16:22:33.196807
ProfileEvent_Query:                                              0
ProfileEvent_SelectQuery:                                        0
ProfileEvent_InsertQuery:                                        0
ProfileEvent_FailedQuery:                                        0
ProfileEvent_FailedSelectQuery:                                  0
...
...
CurrentMetric_Revision:                                          54439
CurrentMetric_VersionInteger:                                    20009001
CurrentMetric_RWLockWaitingReaders:                              0
CurrentMetric_RWLockWaitingWriters:                              0
CurrentMetric_RWLockActiveReaders:                               0
CurrentMetric_RWLockActiveWriters:                               0
CurrentMetric_GlobalThread:                                      74
CurrentMetric_GlobalThreadActive:                                26
CurrentMetric_LocalThread:                                       0
CurrentMetric_LocalThreadActive:                                 0
CurrentMetric_DistributedFilesToInsert:                          0
```

**Schema**
This table can be configured with different schema types using the XML tag `<schema_type>`. The default schema type is `wide`, where each metric or profile event is stored as a separate column. This schema is the most performant and efficient for single-column reads.

The `bucketed` schema stores all profile events and current metrics in a single `metrics` column of type [Map](/reference/data-types/map)([Enum16](/reference/data-types/enum), [Int64](/reference/data-types/int-uint)), so the table consists of a few columns instead of thousands. The `Map` uses the bucketed serialization (`map_serialization_version = 'with_buckets'`) with a constant number of 128 buckets, so reading a single metric reads only one of the 128 buckets. Zero values are not stored: reading a missing key returns `0`. Every metric is also exposed through an `ALIAS` column named as the metric itself (for example, `ProfileEvent_Query UInt64 ALIAS metrics['ProfileEvent_Query']`), so all queries written for the `wide` schema continue to work. Profile events are stored as increments during the collection interval, and current metrics are stored as values at the moment of collection.

```xml
<clickhouse>
    <metric_log>
        <schema_type>bucketed</schema_type>
    </metric_log>
</clickhouse>
```

The `transposed` schema stores data in a format similar to `system.asynchronous_metric_log`, where metrics and events are stored as rows. This schema is useful for low-resource setups because it reduces resource consumption during merges.

**Histograms**

Each row also carries a snapshot of every registered histogram metric in a `histograms` Nested column with fields `metric`, `labels`, `histogram`, `count`, and `sum`. Bucket counts are cumulative since server startup. By default, histograms whose total `count` is zero are not emitted, and zero-counter buckets within an emitted histogram are omitted from the `histogram` map; set `system_metric_log_show_zero_values_in_histograms = 1` (in the default user profile) to keep all histograms and all buckets.

Example query:

```sql
SELECT h.metric, h.labels, h.histogram, h.count, h.sum
FROM system.metric_log
ARRAY JOIN histograms AS h
WHERE h.metric = 'keeper_response_time_ms' AND h.labels['operation_type'] = 'readonly'
ORDER BY event_time DESC
LIMIT 1;
```

.see_also
- [metric_log setting](/reference/settings/server-settings/settings/other#metric_log) — Enabling and disabling the setting.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_ERROR_LOG[] = R"DOCS_MD(
.description
Contains history of error values from table `system.errors`, periodically flushed to disk.

.examples
```sql
SELECT * FROM system.error_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:            clickhouse.testing.internal
event_date:          2025-11-11
event_time:          2025-11-11 11:35:28
code:                60
error:               UNKNOWN_TABLE
value:               1
remote:              0
last_error_time:     2025-11-11 11:35:28
last_error_message:  Unknown table expression identifier 'system.table_not_exist' in scope SELECT * FROM system.table_not_exist
last_error_query_id: 77ad9ece-3db7-4236-9b5a-f789bce4aa2e
last_error_trace:    [100506790044914,100506534488542,100506409937998,100506409936517,100506425182891,100506618154123,100506617994473,100506617990486,100506617988112,100506618341386,100506630272160,100506630266232,100506630276900,100506629795243,100506633519500,100506633495783,100506692143858,100506692248921,100506790779783,100506790781278,100506790390399,100506790380047,123814948752036,123814949330028]
```

.see_also
- [error_log setting](/reference/settings/server-settings/settings/other#error_log) — Enabling and disabling the setting.
- [system.errors](/reference/system-tables/errors) — Contains error codes with the number of times they have been triggered.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_FILESYSTEM_CACHE_LOG[] = R"DOCS_MD(
.description
Contains a history of all events occurred with filesystem cache for objects on a remote filesystem.

It is safe to truncate or drop this table at any time.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_FILESYSTEM_READ_PREFETCHES_LOG[] = R"DOCS_MD(
.description
Contains a history of all prefetches done during reading from MergeTree tables backed by a remote filesystem.

It is safe to truncate or drop this table at any time.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_S3QUEUE_LOG[] = R"DOCS_MD(
.description
Contains log entries with information about files processed by the S3Queue engine.

It is safe to truncate or drop this table at any time.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_AZURE_QUEUE_LOG[] = R"DOCS_MD(
.description
Contains log entries with information about files processed by the AzureQueue engine.

It is safe to truncate or drop this table at any time.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_ASYNCHRONOUS_METRIC_LOG[] = R"DOCS_MD(
.description
Contains the historical values for `system.asynchronous_metrics`, which are saved once per time interval (one second by default). Enabled by default.

Key-value metrics of `system.asynchronous_metrics` (those broken down per CPU core, block device, network interface, or disk) are logged as one row per key, with the key in the `key` column. For scalar metrics the `key` column is empty.

.examples
```sql
SELECT * FROM system.asynchronous_metric_log LIMIT 3 \G
```

```text
Row 1:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:07
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0.001

Row 2:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:08
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0

Row 3:
──────
hostname:   clickhouse.eu-central1.internal
event_date: 2023-11-14
event_time: 2023-11-14 14:39:09
metric:     AsynchronousHeavyMetricsCalculationTimeSpent
key:
value:      0
```

**See Also**

- [asynchronous_metric_log setting](/reference/settings/server-settings/settings/asynchronous#asynchronous_metric_log) — Enabling and disabling the setting.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains metrics, calculated periodically in the background.
- [system.metric_log](/reference/system-tables/metric_log) — Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_OPENTELEMETRY_SPAN_LOG[] = R"DOCS_MD(
.description
Contains information about [trace spans](https://opentracing.io/docs/overview/spans/) for executed queries.

.examples
```sql title="Query"
SELECT * FROM system.opentelemetry_span_log LIMIT 1 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
trace_id:         cdab0847-0d62-61d5-4d38-dd65b19a1914
span_id:          701487461015578150
parent_span_id:   2991972114672045096
operation_name:   DB::Block DB::InterpreterSelectQuery::getSampleBlockImpl()
kind:             INTERNAL
start_time_us:    1612374594529090
finish_time_us:   1612374594529108
finish_date:      2021-02-03
attribute.names:  []
attribute.values: []
```

.see_also
- [OpenTelemetry](/guides/oss/deployment-and-scaling/monitoring/opentelemetry)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_QUERY_VIEWS_LOG[] = R"DOCS_MD(
.description
Contains information about the dependent views executed when running a query, for example, the view type or the execution time.

To start logging:

1. Configure parameters in the [query_views_log](/reference/settings/server-settings/settings/query#query_views_log) section.
2. Set [log_query_views](/reference/settings/session-settings/log-query#log_query_views) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_views_log](/reference/settings/server-settings/settings/query#query_views_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability)) setting to reduce the number of queries, registered in the `query_views_log` table.

.examples
```sql title="Query"
SELECT * FROM system.query_views_log LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
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

.see_also
{/*AUTOGENERATED_START*/}
{/*AUTOGENERATED_END*/}
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_ZOOKEEPER_LOG[] = R"DOCS_MD(
.description
This table contains information about the parameters of the request to the ZooKeeper server and the response from it.

For requests, only columns with request parameters are filled in, and the remaining columns are filled with default values (`0` or `NULL`). When the response arrives, the data from the response is added to the other columns.

.examples
```sql title="Query"
SELECT * FROM system.zookeeper_log WHERE (session_id = '106662742089334927') AND (xid = '10858') FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
hostname:         clickhouse.eu-central1.internal
type:             Request
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.291792
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             0
error:            ᴺᵁᴸᴸ
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:
stat_czxid:       0
stat_mzxid:       0
stat_pzxid:       0
stat_version:     0
stat_cversion:    0
stat_dataLength:  0
stat_numChildren: 0
children:         []

Row 2:
──────
type:             Response
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.292086
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             16926267
error:            ZOK
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:
stat_czxid:       16925469
stat_mzxid:       16925469
stat_pzxid:       16926179
stat_version:     0
stat_cversion:    7
stat_dataLength:  0
stat_numChildren: 7
children:         ['query-0000000006','query-0000000005','query-0000000004','query-0000000003','query-0000000002','query-0000000001','query-0000000000']
```

.see_also
- [ZooKeeper](/guides/oss/best-practices/tips#zookeeper)
- [ZooKeeper guide](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_SESSION_LOG[] = R"DOCS_MD(
.description
Contains information about all successful and failed login and logout events.

<Info>
**Availability**

`system.session_log` is created only when the server configuration contains a `session_log` section. The section is commented out in the default configuration, so queries against the table fail with `UNKNOWN_TABLE` until it is enabled. For example:

```xml
<clickhouse>
    <session_log>
        <database>system</database>
        <table>session_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </session_log>
</clickhouse>
```
</Info>

## Reading rotated tables after an upgrade {#reading-rotated-tables-after-an-upgrade}

When a new value is added to the `interface` enumeration, the table already stored on disk keeps the older `Enum8` definition. On the first start of the new version, `system.session_log` is renamed to `system.session_log_<N>` and a fresh table with the current schema is created in its place; the schema of the rotated table is not changed.

If a login was recorded over an interface that was missing from the enumeration of the version that wrote it — for example an `ArrowFlight` login on a version where `ArrowFlight` was absent from the `interface` enumeration — the rotated table contains a raw value that its own enumeration does not define, and a `SELECT` from it throws `Unexpected value 10 in enum`. Extend the enumeration of that table to read it; this is a metadata-only operation which does not rewrite any data:

```sql
ALTER TABLE system.session_log_1
    MODIFY COLUMN interface Enum8('TCP' = 1, 'HTTP' = 2, 'gRPC' = 3, 'MySQL' = 4, 'PostgreSQL' = 5, 'Local' = 6, 'TCP_Interserver' = 7, 'Prometheus' = 8, 'Background' = 9, 'ArrowFlight' = 10);
```

.examples
```sql title="Query"
SELECT * FROM system.session_log LIMIT 1 FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
type:                    LoginSuccess
auth_id:                 45e6bd83-b4aa-4a23-85e6-bd83b4aa1a23
session_id:
event_date:              2021-10-14
event_time:              2021-10-14 20:33:52
event_time_microseconds: 2021-10-14 20:33:52.104247
user:                    default
auth_type:               PLAINTEXT_PASSWORD
profiles:                ['default']
roles:                   []
settings:                [('load_balancing','random'),('max_memory_usage','10000000000')]
client_address:          ::ffff:127.0.0.1
client_port:             38490
interface:               TCP
client_hostname:
client_name:             ClickHouse client
client_revision:         54449
client_version_major:    21
client_version_minor:    10
client_version_patch:    0
failure_reason:
certificate_subjects:    []
certificate_serial:
certificate_issuer:
certificate_not_before:  ᴺᵁᴸᴸ
certificate_not_after:   ᴺᵁᴸᴸ
```
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_TRANSACTIONS_INFO_LOG[] = R"DOCS_MD(
.description
Contains information about all transactions executed on a current server.

<Info>
**Availability**

`system.transactions_info_log` is created only when the server configuration contains a `transactions_info_log` section. Transactions must also be enabled with `allow_experimental_transactions` for the table to accumulate rows. Without the log section, queries against the table fail with `UNKNOWN_TABLE`; without experimental transactions, the configured table remains empty.

```xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
    <transactions_info_log>
        <database>system</database>
        <table>transactions_info_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </transactions_info_log>
</clickhouse>
```
</Info>

It is safe to truncate or drop this table at any time.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_PROCESSORS_PROFILE_LOG[] = R"DOCS_MD(
.description
This table contains profiling on processors level (that you can find in [`EXPLAIN PIPELINE`](/reference/statements/explain#explain-pipeline)).

.examples
```sql title="Query"
EXPLAIN PIPELINE
SELECT sleep(1)
┌─explain─────────────────────────┐
│ (Expression)                    │
│ ExpressionTransform             │
│   (SettingQuotaAndLimits)       │
│     (ReadFromStorage)           │
│     SourceFromSingleChunk 0 → 1 │
└─────────────────────────────────┘

SELECT sleep(1)
SETTINGS log_processors_profiles = 1
Query id: feb5ed16-1c24-4227-aa54-78c02b3b27d4
┌─sleep(1)─┐
│        0 │
└──────────┘
1 rows in set. Elapsed: 1.018 sec.

SELECT
    name,
    elapsed_us,
    input_wait_elapsed_us,
    output_wait_elapsed_us
FROM system.processors_profile_log
WHERE query_id = 'feb5ed16-1c24-4227-aa54-78c02b3b27d4'
ORDER BY name ASC
```

```text title="Response"
┌─name────────────────────┬─elapsed_us─┬─input_wait_elapsed_us─┬─output_wait_elapsed_us─┐
│ ExpressionTransform     │    1000497 │                  2823 │                    197 │
│ LazyOutputFormat        │         36 │               1002188 │                      0 │
│ LimitsCheckingTransform │         10 │               1002994 │                    106 │
│ NullSource              │          5 │               1002074 │                      0 │
│ NullSource              │          1 │               1002084 │                      0 │
│ SourceFromSingleChunk   │         45 │                  4736 │                1000819 │
└─────────────────────────┴────────────┴───────────────────────┴────────────────────────┘
```

Here you can see:

- `ExpressionTransform` was executing `sleep(1)` function, so it `work` will takes 1e6, and so `elapsed_us` > 1e6.
- `SourceFromSingleChunk` need to wait, because `ExpressionTransform` does not accept any data during execution of `sleep(1)`, so it will be in `PortFull` state for 1e6 us, and so `output_wait_elapsed_us` > 1e6.
- `LimitsCheckingTransform`/`NullSource`/`LazyOutputFormat` need to wait until `ExpressionTransform` will execute `sleep(1)` to process the result, so `input_wait_elapsed_us` > 1e6.

.see_also
- [`EXPLAIN PIPELINE`](/reference/statements/explain#explain-pipeline)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_ASYNCHRONOUS_INSERT_LOG[] = R"DOCS_MD(
.description
Contains information about async inserts. Each entry represents an insert query buffered into an async insert query.

To start logging configure parameters in the [asynchronous_insert_log](/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log) section.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [asynchronous_insert_log](/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

.examples
```sql title="Query"
SELECT * FROM system.asynchronous_insert_log LIMIT 1 \G;
```

```text title="Response"
hostname:                clickhouse.eu-central1.internal
event_date:              2023-06-08
event_time:              2023-06-08 10:08:53
event_time_microseconds: 2023-06-08 10:08:53.199516
query:                   INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:                public
table:                   data_guess
format:                  CSV
query_id:                b46cd4c4-0269-4d0b-99f5-d27668c6102e
bytes:                   133223
exception:
status:                  Ok
flush_time:              2023-06-08 10:08:55
flush_time_microseconds: 2023-06-08 10:08:55.139676
flush_query_id:          cd2c1e43-83f5-49dc-92e4-2fbc7f8d3716
```

.see_also
- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_inserts](/reference/system-tables/asynchronous_inserts) — This table contains information about pending asynchronous inserts in queue.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_BACKUP_LOG[] = R"DOCS_MD(
.description
Contains logging entries with information about `BACKUP` and `RESTORE` operations.

.examples
```sql
BACKUP TABLE test_db.my_table TO Disk('backups_disk', '1.zip')
```

```response
┌─id───────────────────────────────────┬─status─────────┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ BACKUP_CREATED │
└──────────────────────────────────────┴────────────────┘
```

```sql
SELECT hostname, event_date, event_time_microseconds, id, name, status, error, start_time, end_time, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read FROM system.backup_log WHERE id = 'e5b74ecb-f6f1-426a-80be-872f90043885' ORDER BY event_date, event_time_microseconds \G
```

```response
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time_microseconds: 2023-08-19 11:05:21.998566
id:                      e5b74ecb-f6f1-426a-80be-872f90043885
name:                    Disk('backups_disk', '1.zip')
status:                  CREATING_BACKUP
error:
start_time:              2023-08-19 11:05:21
end_time:                1970-01-01 03:00:00
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

Row 2:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time:              2023-08-19 11:08:56
event_time_microseconds: 2023-08-19 11:08:56.916192
id:                      e5b74ecb-f6f1-426a-80be-872f90043885
name:                    Disk('backups_disk', '1.zip')
status:                  BACKUP_CREATED
error:
start_time:              2023-08-19 11:05:21
end_time:                2023-08-19 11:08:56
num_files:               57
total_size:              4290364870
num_entries:             46
uncompressed_size:       4290362365
compressed_size:         3525068304
files_read:              0
bytes_read:              0
```
```sql
RESTORE TABLE test_db.my_table FROM Disk('backups_disk', '1.zip')
```

```response
┌─id───────────────────────────────────┬─status───┐
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ RESTORED │
└──────────────────────────────────────┴──────────┘
```

```sql
SELECT hostname, event_date, event_time_microseconds, id, name, status, error, start_time, end_time, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read FROM system.backup_log WHERE id = 'cdf1f731-52ef-42da-bc65-2e1bfcd4ce90' ORDER BY event_date, event_time_microseconds \G
```

```response
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time_microseconds: 2023-08-19 11:09:19.718077
id:                      cdf1f731-52ef-42da-bc65-2e1bfcd4ce90
name:                    Disk('backups_disk', '1.zip')
status:                  RESTORING
error:
start_time:              2023-08-19 11:09:19
end_time:                1970-01-01 03:00:00
num_files:               0
total_size:              0
num_entries:             0
uncompressed_size:       0
compressed_size:         0
files_read:              0
bytes_read:              0

Row 2:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-08-19
event_time_microseconds: 2023-08-19 11:09:29.334234
id:                      cdf1f731-52ef-42da-bc65-2e1bfcd4ce90
name:                    Disk('backups_disk', '1.zip')
status:                  RESTORED
error:
start_time:              2023-08-19 11:09:19
end_time:                2023-08-19 11:09:29
num_files:               57
total_size:              4290364870
num_entries:             46
uncompressed_size:       4290362365
compressed_size:         4290362365
files_read:              57
bytes_read:              4290364870
```

This is essentially the same information that is written in the system table `system.backups`:

```sql
SELECT id, name, status, error, start_time, end_time, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read FROM system.backups ORDER BY start_time
```

```response
┌─id───────────────────────────────────┬─name──────────────────────────┬─status─────────┬─error─┬──────────start_time─┬────────────end_time─┬─num_files─┬─total_size─┬─num_entries─┬─uncompressed_size─┬─compressed_size─┬─files_read─┬─bytes_read─┐
│ e5b74ecb-f6f1-426a-80be-872f90043885 │ Disk('backups_disk', '1.zip') │ BACKUP_CREATED │       │ 2023-08-19 11:05:21 │ 2023-08-19 11:08:56 │        57 │ 4290364870 │          46 │        4290362365 │      3525068304 │          0 │          0 │
│ cdf1f731-52ef-42da-bc65-2e1bfcd4ce90 │ Disk('backups_disk', '1.zip') │ RESTORED       │       │ 2023-08-19 11:09:19 │ 2023-08-19 11:09:29 │        57 │ 4290364870 │          46 │        4290362365 │      4290362365 │         57 │ 4290364870 │
└──────────────────────────────────────┴───────────────────────────────┴────────────────┴───────┴─────────────────────┴─────────────────────┴───────────┴────────────┴─────────────┴───────────────────┴─────────────────┴────────────┴────────────┘
```

.see_also
- [Backup and Restore](/concepts/features/backup-restore/overview)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_BLOB_STORAGE_LOG[] = R"DOCS_MD(
.description
Contains logging entries with information about various blob storage operations such as uploads and deletes.

.examples
Suppose a blob storage operation uploads a file, and an event is logged:

```sql
SELECT * FROM system.blob_storage_log WHERE query_id = '7afe0450-504d-4e4b-9a80-cd9826047972' ORDER BY event_date, event_time_microseconds \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2023-10-31
event_time:              2023-10-31 16:03:40
event_time_microseconds: 2023-10-31 16:03:40.481437
event_type:              Upload
query_id:                7afe0450-504d-4e4b-9a80-cd9826047972
thread_id:               2381740
disk_name:               disk_s3
bucket:                  bucket1
remote_path:             rrr/kxo/tbnqtrghgtnxkzgtcrlutwuslgawe
local_path:              store/654/6549e8b3-d753-4447-8047-d462df6e6dbe/tmp_insert_all_1_1_0/checksums.txt
data_size:               259
error:
```

In this example, upload operation was associated with the `INSERT` query with ID `7afe0450-504d-4e4b-9a80-cd9826047972`. The local metadata file `store/654/6549e8b3-d753-4447-8047-d462df6e6dbe/tmp_insert_all_1_1_0/checksums.txt` refers to remote path `rrr/kxo/tbnqtrghgtnxkzgtcrlutwuslgawe` in bucket `bucket1` on disk `disk_s3`, with a size of 259 bytes.

.see_also
- [External Disks for Storing Data](/concepts/features/configuration/server-config/storing-data)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_QUERY_METRIC_LOG[] = R"DOCS_MD(
.description
Contains a history of memory and metric values from table `system.events` for individual queries, periodically flushed to disk.

Once a query starts, data is collected at periodic intervals of `query_metric_log_interval` milliseconds (which is set to 1000
by default). The data is also collected when the query finishes if the query takes longer than `query_metric_log_interval`.

.examples
```sql
SELECT * FROM system.query_metric_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
query_id:                                                        97c8ba04-b6d4-4bd7-b13e-6201c5c6e49d
hostname:                                                        clickhouse.eu-central1.internal
event_date:                                                      2020-09-05
event_time:                                                      2020-09-05 16:22:33
event_time_microseconds:                                         2020-09-05 16:22:33.196807
memory_usage:                                                    313434219
peak_memory_usage:                                               598951986
ProfileEvent_Query:                                              0
ProfileEvent_SelectQuery:                                        0
ProfileEvent_InsertQuery:                                        0
ProfileEvent_FailedQuery:                                        0
ProfileEvent_FailedSelectQuery:                                  0
...
```

.see_also
- [query_metric_log setting](/reference/settings/server-settings/settings/query#query_metric_log) — Enabling and disabling the setting.
- [query_metric_log_interval](/reference/settings/session-settings/other#query_metric_log_interval)
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_DEAD_LETTER_QUEUE[] = R"DOCS_MD(
.description
Contains information about messages received via a streaming engine and parsed with errors. Currently implemented for Kafka and RabbitMQ.

<Info>
**Availability**

`system.dead_letter_queue` is created only when the server configuration contains a `dead_letter_queue` section. The streaming engine must also set its engine-specific `handle_error_mode` setting to `dead_letter_queue` to write rejected messages. Without the server configuration section, queries against the table fail with `UNKNOWN_TABLE`.

```xml
<clickhouse>
    <dead_letter_queue>
        <database>system</database>
        <table>dead_letter_queue</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </dead_letter_queue>
</clickhouse>
```
</Info>

Logging is enabled by specifying `dead_letter_queue` for the engine specific `handle_error_mode` setting.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [dead_letter_queue](/reference/settings/server-settings/settings/other#dead_letter_queue) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

.examples
```sql title="Query"
SELECT * FROM system.dead_letter_queue LIMIT 1 \G;
```

```text title="Response"
Row 1:
──────
table_engine:                  Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.910773
database:                      default
table:                         kafka
error:                         Cannot parse input: expected '\t' before: 'qwertyuiop': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "qwertyuiop" is not like UInt64
raw_message:                   qwertyuiop
kafka_topic_name:              TSV_dead_letter_queue_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:

Row 2:
──────
table_engine:                  Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.910944
database:                      default
table:                         kafka
error:                         Cannot parse input: expected '\t' before: 'asdfghjkl': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "asdfghjkl" is not like UInt64
raw_message:                   asdfghjkl
kafka_topic_name:              TSV_dead_letter_queue_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:

Row 3:
──────
table_engine:                  Kafka
event_date:                    2025-05-01
event_time:                    2025-05-01 10:34:53
event_time_microseconds:       2025-05-01 10:34:53.911092
database:                      default
table:                         kafka
error:                         Cannot parse input: expected '\t' before: 'zxcvbnm': (at row 1)
:
Row 1:
Column 0,   name: key,   type: UInt64, ERROR: text "zxcvbnm" is not like UInt64
raw_message:                   zxcvbnm
kafka_topic_name:              TSV_dead_letter_queue_err_1746095689
kafka_partition:               0
kafka_offset:                  0
kafka_key:
rabbitmq_exchange_name:
rabbitmq_message_id:
rabbitmq_message_timestamp:    1970-01-01 00:00:00
rabbitmq_message_redelivered:  0
rabbitmq_message_delivery_tag: 0
rabbitmq_channel_id:
 (test.py:78, dead_letter_queue_test)

```

.see_also
- [Kafka](/reference/engines/table-engines/integrations/kafka) - Kafka Engine
- [system.kafka_consumers](/reference/system-tables/kafka_consumers) — Description of the `kafka_consumers` system table which contains information like statistics and errors about Kafka consumers.
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_ZOOKEEPER_CONNECTION_LOG[] = R"DOCS_MD(
.description
The 'system.zookeeper_connection_log' table shows the history of ZooKeeper connections (including auxiliary ZooKeepers). Each row shows information about one event regarding connections.

<Note>
The table doesn't contain events for disconnections caused by server shutdown.
</Note>

.examples
```sql
SELECT * FROM system.zookeeper_connection_log;
```

```text
    ┌─hostname─┬─type─────────┬─event_date─┬──────────event_time─┬────event_time_microseconds─┬─name───────────────┬─host─┬─port─┬─index─┬─client_id─┬─keeper_api_version─┬─enabled_feature_flags───────────────────────────────────────────────────────────────────────┬─availability_zone─┬─reason──────────────┐
 1. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:35 │ 2025-05-12 19:49:35.713067 │ zk_conn_log_test_4 │ zoo2 │ 2181 │     0 │        10 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 2. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:23 │ 2025-05-12 19:49:23.981570 │ default            │ zoo1 │ 2181 │     0 │         4 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 3. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:28 │ 2025-05-12 19:49:28.104021 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 4. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.459251 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 5. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.574312 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Initialization      │
 6. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909890 │ default            │ zoo1 │ 2181 │     0 │         5 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 7. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.909895 │ default            │ zoo2 │ 2181 │     0 │         8 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 8. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912010 │ zk_conn_log_test_2 │ zoo2 │ 2181 │     0 │         6 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
 9. │ node     │ Connected    │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912014 │ zk_conn_log_test_2 │ zoo3 │ 2181 │     0 │         9 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Config changed      │
10. │ node     │ Disconnected │ 2025-05-12 │ 2025-05-12 19:49:29 │ 2025-05-12 19:49:29.912061 │ zk_conn_log_test_3 │ zoo3 │ 2181 │     0 │         7 │                  0 │ ['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE'] │                   │ Removed from config │
    └──────────┴──────────────┴────────────┴─────────────────────┴────────────────────────────┴────────────────────┴──────┴──────┴───────┴───────────┴────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────┴─────────────────────┘
```
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_AGGREGATED_ZOOKEEPER_LOG[] = R"DOCS_MD(
.description
This table contains aggregated statistics of ZooKeeper operations (e.g. number of operations, average latency, errors) grouped by `(session_id, parent_path, operation, component, is_subrequest)` and periodically flushed to disk.

Unlike [system.zookeeper_log](/reference/system-tables/zookeeper_log) which logs every individual request and response, this table aggregates operations into groups, making it much more lightweight and therefore more suitable for production workloads.

Operations that are part of a `Multi` or `MultiRead` batch are tracked separately via the `is_subrequest` column. Subrequests have zero latency because the total latency is attributed to the enclosing `Multi`/`MultiRead` operation.

.see_also
- [system.zookeeper_log](/reference/system-tables/zookeeper_log) — Detailed per-request ZooKeeper log.
- [ZooKeeper](/guides/oss/best-practices/tips#zookeeper)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_ICEBERG_METADATA_LOG[] = R"DOCS_MD(
.description
The `system.iceberg_metadata_log` table records metadata access and parsing events for Iceberg tables read by ClickHouse. It provides detailed information about each metadata file or entry processed, which is useful for debugging, auditing, and understanding Iceberg table structure evolution.

This table logs every metadata file and entry read from Iceberg tables, including root metadata files, manifest lists, and manifest entries. It helps users trace how ClickHouse interprets Iceberg table metadata and diagnose issues related to schema evolution, file resolution, or query planning.

<Note>
This table is primarily intended for debugging purposes.
</Note>

### Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`iceberg_metadata_log_level`](/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'manifest_file_entry';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

To log only the root metadata JSON file used in the current query:

```sql
SELECT * FROM my_iceberg_table SETTINGS iceberg_metadata_log_level = 'metadata';

SYSTEM FLUSH LOGS iceberg_metadata_log;

SELECT content_type, file_path, row_in_file
FROM system.iceberg_metadata_log
WHERE query_id = '{previous_query_id}';
```

See more information in the description of the [`iceberg_metadata_log_level`](/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level) setting.

### Good To Know {#good-to-know}

- Use `iceberg_metadata_log_level` at the query level only when you need to investigate your Iceberg table in detail. Otherwise, you may populate the log table with excessive metadata and experience performance degradation.
- The table contains duplicate entries, as it is intended primarily for debugging and does not guarantee uniqueness per entity. Separate rows store content and pruning status because they are collected at different moments in a program. Content is collected when the metadata is read, pruning status is collected when the metadata is checked for pruning. **Never rely on the table itself for deduplication.**
- If you use a `content_type` more verbose than `ManifestListMetadata`, the Iceberg metadata cache is disabled for manifest lists.
- Similarly, if you use a `content_type` more verbose than `ManifestFileMetadata`, the Iceberg metadata cache is disabled for manifest files.
- If the SELECT query was cancelled or failed, the log table may still contain entries for metadata processed before the failure but will not contain information about metadata entities that were not processed.

.columns_notes
### `content_type` values {#content-type-values}

- `None`: No content.
- `Metadata`: Root metadata file.
- `ManifestListMetadata`: Manifest list metadata.
- `ManifestListEntry`: Entry in a manifest list.
- `ManifestFileMetadata`: Manifest file metadata.
- `ManifestFileEntry`: Entry in a manifest file.

.see_also
- [Iceberg Table Engine](/reference/engines/table-engines/integrations/iceberg)
- [Iceberg Table Function](/reference/functions/table-functions/iceberg)
- [system.iceberg_history](/reference/system-tables/iceberg_history)
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_DELTA_LAKE_METADATA_LOG[] = R"DOCS_MD(
.description
The `system.delta_lake_metadata_log` table records metadata access and parsing events for Delta Lake tables read by ClickHouse. It provides detailed information about each metadata file, which is useful for debugging, auditing, and understanding Delta table structure evolution.

This table logs every metadata file read from Delta Lake tables. It helps users trace how ClickHouse interprets Delta table metadata and diagnose issues related to schema evolution, snapshot resolution, or query planning.

<Note>
This table is primarily intended for debugging purposes.
</Note>

### Controlling log verbosity {#controlling-log-verbosity}

You can control which metadata events are logged using the [`delta_lake_log_metadata`](/reference/settings/session-settings/delta-lake#delta_lake_log_metadata) setting.

To log all metadata used in the current query:

```sql
SELECT * FROM my_delta_table SETTINGS delta_lake_log_metadata = 1;

SYSTEM FLUSH LOGS delta_lake_metadata_log;

SELECT *
FROM system.delta_lake_metadata_log
WHERE query_id = '{previous_query_id}';
```
)DOCS_MD";

inline constexpr char SYSTEM_LOG_DOCUMENTATION_PREDICATE_STATISTICS_LOG[] = R"DOCS_MD(
.description
Contains sampled selectivity statistics collected while reading from `MergeTree` tables. The table is populated only when [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate) is greater than `0`.

<Info>
**Availability**

`system.predicate_statistics_log` is created only when the server configuration contains a `predicate_statistics_log` section. After creating the log, set `predicate_statistics_sample_rate` to a value greater than `0` to collect rows. Without the log section, queries against the table fail with `UNKNOWN_TABLE`.

```xml
<clickhouse>
    <predicate_statistics_log>
        <database>system</database>
        <table>predicate_statistics_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </predicate_statistics_log>
</clickhouse>
```
</Info>

Use this table to inspect how selective user predicates are in real workloads and how many granules remain after primary-key or skip-index filtering. The data is intended as input for workload-driven index and projection recommendations.

## Row shapes {#row-shapes}

A single query can produce two kinds of rows in `system.predicate_statistics_log`:

- **Filter rows**, emitted per prewhere/filter step in `MergeTreeSelectProcessor`. They populate `predicate_expression`, `input_rows`, `passed_rows`, `filter_selectivity`, and the whole-predicate columns `total_input_rows`, `total_passed_rows`, `total_selectivity`. Index-related columns are empty.
- **Index rows**, emitted per read step in `ReadFromMergeTree`. They populate the `index_names`, `index_types`, `total_granules`, `granules_after`, and `index_selectivities` arrays, one entry per index stage (primary key, partition, skip indexes). Predicate-related columns are empty.

Filter rows and index rows for the same query share the same `query_id` and `table`, so they can be joined when both are needed.

## Sampling and overhead {#sampling-and-overhead}

Sampling is controlled by [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate):

- `0` disables collection.
- `1` samples every query.
- `N > 1` samples approximately `1 / N` of queries, hashed by `query_id`.

Lower values produce more data but add CPU work on the read path and more writes to the system log. After enabling the setting, use [`SYSTEM FLUSH LOGS`](/reference/statements/system#flush-logs) if you need rows to appear immediately.

.examples
```sql
SET predicate_statistics_sample_rate = 1;

SELECT *
FROM hits
WHERE URL LIKE '%/product/%' AND EventDate >= today() - 7
FORMAT Null;

SYSTEM FLUSH LOGS predicate_statistics_log;

SELECT
    query_id,
    predicate_expression,
    round(filter_selectivity, 3) AS step_selectivity,
    round(total_selectivity, 3) AS query_selectivity,
    index_names,
    index_selectivities
FROM system.predicate_statistics_log
WHERE table = 'hits'
ORDER BY event_time DESC
LIMIT 10;
```

.see_also
- [`predicate_statistics_sample_rate`](/reference/settings/session-settings/other#predicate_statistics_sample_rate)
- [system.query_log](/reference/system-tables/query_log)
)DOCS_MD";

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


class StorageWithComment : public IAST
{
public:
    ASTPtr storage;
    ASTPtr comment;

    String getID(char) const override { return "Storage with comment definition"; }

    ASTPtr clone() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method clone is not supported");
    }

protected:
    void formatImpl(WriteBuffer &, const FormatSettings &, FormatState &, FormatStateStacked) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported");
    }
};

class ParserStorageWithComment : public IParserBase
{
protected:
    const char * getName() const override { return "storage definition with comment"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
        ASTPtr storage;

        if (!storage_p.parse(pos, storage, expected))
            return false;

        ParserKeyword s_comment(Keyword::COMMENT);
        ParserStringLiteral string_literal_parser;
        ASTPtr comment;

        if (s_comment.ignore(pos, expected))
            string_literal_parser.parse(pos, comment, expected);

        auto storage_with_comment = make_intrusive<StorageWithComment>();
        storage_with_comment->storage = std::move(storage);
        storage_with_comment->comment = std::move(comment);

        node = storage_with_comment;
        return true;
    }
};

/** Allow to store structured log in system table.
  *
  * Logging is asynchronous. Data is put into queue from where it will be read by separate thread.
  * That thread inserts log into a table with no more than specified periodicity.
  */

/** Structure of log, template parameter.
  * Structure could change on server version update.
  * If on first write, existing table has different structure,
  *  then it get renamed (put aside) and new table is created.
  */
/* Example:
    struct LogElement
    {
        /// default constructor must be available
        /// fields

        static std::string name();
        static ColumnsDescription getColumnsDescription();
        /// TODO: Remove this method, we can return aliases directly from getColumnsDescription().
        static NamesAndAliases getNamesAndAliases();
        void appendToBlock(MutableColumns & columns) const;
    };
    */

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define FORWARD_DECLARATION(log_type, member, descr) \
    class log_type; \

LIST_OF_ALL_SYSTEM_LOGS(FORWARD_DECLARATION)
#if CLICKHOUSE_CLOUD
    LIST_OF_CLOUD_SYSTEM_LOGS(FORWARD_DECLARATION)
#endif
#undef FORWARD_DECLARATION
/// NOLINTEND(bugprone-macro-parentheses)

/// Returns `true` if the configuration contains any system log section
/// (e.g. `query_log`, `processors_profile_log`).
bool hasAnySystemLogConfigured(const Poco::Util::AbstractConfiguration & config);

/// System logs should be destroyed in destructor of the last Context and before tables,
///  because SystemLog destruction makes insert query while flushing data into underlying tables
class SystemLogs
{
public:
    SystemLogs() = default;
    SystemLogs(ContextPtr global_context, const Poco::Util::AbstractConfiguration & config);
    SystemLogs(const SystemLogs & other) = default;

    void flush(const std::vector<std::pair<String, String>> & names);
    void flushAndShutdown();
    void shutdown();
    void handleCrash();

#define DECLARE_PUBLIC_MEMBERS(log_type, member, descr) \
    std::shared_ptr<log_type> member; \

    LIST_OF_ALL_SYSTEM_LOGS(DECLARE_PUBLIC_MEMBERS)
    #if CLICKHOUSE_CLOUD
        LIST_OF_CLOUD_SYSTEM_LOGS(DECLARE_PUBLIC_MEMBERS)
    #endif
#undef DECLARE_PUBLIC_MEMBERS

private:
    std::vector<ISystemLog *> getAllLogs() const;

    void flushImpl(const std::vector<std::pair<String, String>>  & names, bool should_prepare_tables_anyway, bool ignore_errors);
};

struct SystemLogSettings
{
    SystemLogQueueSettings queue_settings;

    String engine;
    bool symbolize_traces = false;

    /// Settings of the `all_...` union table over the log table, its rotated versions and/or
    /// the same tables across a cluster. See the `create_union_system_log_tables` section
    /// of the server configuration. The union table is created when at least one of the
    /// two fields below is set.
    bool union_table_merge_rotated_tables = false;
    String union_table_cluster;
};

template <typename LogElement>
class SystemLog : public SystemLogBase<LogElement>, private boost::noncopyable, public WithContext
{
public:
    using Self = SystemLog;
    using Base = SystemLogBase<LogElement>;
    using Element = LogElement;

    /** Parameter: table name where to write log.
      * If table is not exists, then it get created with specified engine.
      * If it already exists, then its structure is checked to be compatible with structure of log record.
      *  If it is compatible, then existing table will be used.
      *  If not - then existing table will be renamed to same name but with suffix '_N' at end,
      *   where N - is a minimal number from 1, for that table with corresponding name doesn't exist yet;
      *   and new table get created - as if previous table was not exist.
      */
    SystemLog(ContextPtr context_,
              const SystemLogSettings & settings_,
              std::shared_ptr<SystemLogQueue<LogElement>> queue_ = nullptr);

    /// Join the saving thread before any derived state (`log`, `flush_policy`, `table_id`, ...)
    /// is destroyed. `savingThreadFunction` is overridden here and reads those members, so the
    /// join must happen at this level rather than in `~SystemLogBase`. Required for paths that
    /// bypass `shutdown` (for example, when an exception escaped `flushAndShutdown` and left
    /// the saving threads running until `~ContextSharedPart`).
    ~SystemLog() override;

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */

    void shutdown() override;

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable() override;

    const StorageID & getTableID() const { return table_id; }

    ISystemLogFlushPolicy & getFlushPolicy() { return *flush_policy; }

    void setManualFlushTargetIndex(ISystemLog::Index target_index) override
    {
        flush_policy->prepareManualFlush(target_index);
    }

protected:
    LoggerPtr log;

    using Base::queue;

    StoragePtr getStorage() const;

    /// Some tables can override settings for internal queries
    virtual void addSettingsForQuery(ContextMutablePtr & mutable_context, IAST::QueryKind query_kind) const;

private:
    /* Saving thread data */
    const StorageID table_id;
    /// The `all_...` table over the log table, its rotated versions and/or the same tables
    /// across a cluster (see `create_union_system_log_tables` in the server config).
    const StorageID union_table_id;
    const String storage_def;
    const bool union_table_merge_rotated_tables;
    const String union_table_cluster;
    std::unique_ptr<ISystemLogFlushPolicy> flush_policy;
    String create_query;
    String old_create_query;
    /// Expected CREATE query of the union table. Empty when the union table is not configured.
    String union_create_query;
    bool is_prepared = false;
    /// Whether the definition of the union table has to be verified against the expected one.
    /// This is done at the first flush and after each rotation of the log table; on other
    /// flushes the union table is only recreated if it went missing (e.g. dropped by a user).
    bool union_table_check_pending = true;
    /// Set when the union table cannot be created (e.g. the configured cluster does not exist)
    /// or the database engine does not support it, to avoid retrying the creation and polluting
    /// the log on every flush. Reset on rotation of the log table.
    bool union_table_broken = false;

    void savingThreadFunction() override;

    /// flushImpl can be executed only in saving_thread.
    void flushImpl(const std::vector<LogElement> & to_flush, uint64_t to_flush_end);
    ASTPtr getCreateTableQuery();
    ASTPtr getCreateUnionTableQuery();

    /// Creates or updates the `all_...` union table if it is configured.
    /// The table is stateless (a proxy over the `merge`/`clusterAllReplicas` table functions),
    /// so it is recreated with an atomic exchange whenever its definition differs from the
    /// expected one. Failures are logged and never interfere with flushing the log itself.
    void prepareUnionTable();
};

}
