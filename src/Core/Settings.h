#pragma once

#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/Defines.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace boost::program_options
{
    class options_description;
}


namespace DB
{
class IColumn;


/** List of settings: type, name, default value, description, flags
  *
  * This looks rather unconvenient. It is done that way to avoid repeating settings in different places.
  * Note: as an alternative, we could implement settings to be completely dynamic in form of map: String -> Field,
  *  but we are not going to do it, because settings is used everywhere as static struct fields.
  *
  * `flags` can be either 0 or IMPORTANT.
  * A setting is "IMPORTANT" if it affects the results of queries and can't be ignored by older versions.
  */

#define COMMON_SETTINGS(M) \
    M(UInt64, min_compress_block_size, 65536, "The actual size of the block to compress, if the uncompressed data less than max_compress_block_size is no less than this value and no less than the volume of data for one mark.", 0) \
    M(UInt64, max_compress_block_size, 1048576, "The maximum size of blocks of uncompressed data before compressing for writing to a table.", 0) \
    M(UInt64, max_block_size, DEFAULT_BLOCK_SIZE, "Maximum block size for reading", 0) \
    M(UInt64, max_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE, "The maximum block size for insertion, if we control the creation of blocks for insertion.", 0) \
    M(UInt64, min_insert_block_size_rows, DEFAULT_INSERT_BLOCK_SIZE, "Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.", 0) \
    M(UInt64, min_insert_block_size_bytes, (DEFAULT_INSERT_BLOCK_SIZE * 256), "Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.", 0) \
    M(UInt64, min_insert_block_size_rows_for_materialized_views, 0, "Like min_insert_block_size_rows, but applied only during pushing to MATERIALIZED VIEW (default: min_insert_block_size_rows)", 0) \
    M(UInt64, min_insert_block_size_bytes_for_materialized_views, 0, "Like min_insert_block_size_bytes, but applied only during pushing to MATERIALIZED VIEW (default: min_insert_block_size_bytes)", 0) \
    M(UInt64, max_joined_block_size_rows, DEFAULT_BLOCK_SIZE, "Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.", 0) \
    M(UInt64, max_insert_threads, 0, "The maximum number of threads to execute the INSERT SELECT query. Values 0 or 1 means that INSERT SELECT is not run in parallel. Higher values will lead to higher memory usage. Parallel INSERT SELECT has effect only if the SELECT part is run on parallel, see 'max_threads' setting.", 0) \
    M(UInt64, max_final_threads, 16, "The maximum number of threads to read from table with FINAL.", 0) \
    M(MaxThreads, max_threads, 0, "The maximum number of threads to execute the request. By default, it is determined automatically.", 0) \
    M(MaxThreads, max_alter_threads, 0, "The maximum number of threads to execute the ALTER requests. By default, it is determined automatically.", 0) \
    M(UInt64, max_read_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, "The maximum size of the buffer to read from the filesystem.", 0) \
    M(UInt64, max_distributed_connections, 1024, "The maximum number of connections for distributed processing of one query (should be greater than max_threads).", 0) \
    M(UInt64, max_query_size, 262144, "Which part of the query can be read into RAM for parsing (the remaining data for INSERT, if any, is read later)", 0) \
    M(UInt64, interactive_delay, 100000, "The interval in microseconds to check if the request is cancelled, and to send progress info.", 0) \
    M(Seconds, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connection timeout if there are no replicas.", 0) \
    M(Milliseconds, connect_timeout_with_failover_ms, DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS, "Connection timeout for selecting first healthy replica.", 0) \
    M(Milliseconds, connect_timeout_with_failover_secure_ms, DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_SECURE_MS, "Connection timeout for selecting first healthy replica (for secure connections).", 0) \
    M(Seconds, receive_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "", 0) \
    M(Seconds, send_timeout, DBMS_DEFAULT_SEND_TIMEOUT_SEC, "", 0) \
    M(Seconds, tcp_keep_alive_timeout, 0, "The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes", 0) \
    M(Milliseconds, queue_max_wait_ms, 0, "The wait time in the request queue, if the number of concurrent requests exceeds the maximum.", 0) \
    M(Milliseconds, connection_pool_max_wait_ms, 0, "The wait time when the connection pool is full.", 0) \
    M(Milliseconds, replace_running_query_max_wait_ms, 5000, "The wait time for running query with the same query_id to finish when setting 'replace_running_query' is active.", 0) \
    M(Milliseconds, kafka_max_wait_ms, 5000, "The wait time for reading from Kafka before retry.", 0) \
    M(Milliseconds, rabbitmq_max_wait_ms, 5000, "The wait time for reading from RabbitMQ before retry.", 0) \
    M(UInt64, poll_interval, DBMS_DEFAULT_POLL_INTERVAL, "Block at the query wait loop on the server for the specified number of seconds.", 0) \
    M(UInt64, idle_connection_timeout, 3600, "Close idle TCP connections after specified number of seconds.", 0) \
    M(UInt64, distributed_connections_pool_size, DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE, "Maximum number of connections with one remote server in the pool.", 0) \
    M(UInt64, connections_with_failover_max_tries, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES, "The maximum number of attempts to connect to replicas.", 0) \
    M(UInt64, s3_min_upload_part_size, 512*1024*1024, "The minimum size of part to upload during multipart upload to S3.", 0) \
    M(Bool, extremes, false, "Calculate minimums and maximums of the result columns. They can be output in JSON-formats.", IMPORTANT) \
    M(Bool, use_uncompressed_cache, true, "Whether to use the cache of uncompressed blocks.", 0) \
    M(Bool, replace_running_query, false, "Whether the running request should be canceled with the same id as the new one.", 0) \
    M(UInt64, background_buffer_flush_schedule_pool_size, 16, "Number of threads performing background flush for tables with Buffer engine. Only has meaning at server startup.", 0) \
    M(UInt64, background_pool_size, 16, "Number of threads performing background work for tables (for example, merging in merge tree). Only has meaning at server startup.", 0) \
    M(UInt64, background_move_pool_size, 8, "Number of threads performing background moves for tables. Only has meaning at server startup.", 0) \
    M(UInt64, background_schedule_pool_size, 16, "Number of threads performing background tasks for replicated tables, kafka streaming, dns cache updates. Only has meaning at server startup.", 0) \
    M(UInt64, background_distributed_schedule_pool_size, 16, "Number of threads performing background tasks for distributed sends. Only has meaning at server startup.", 0) \
    \
    M(Milliseconds, distributed_directory_monitor_sleep_time_ms, 100, "Sleep time for StorageDistributed DirectoryMonitors, in case of any errors delay grows exponentially.", 0) \
    M(Milliseconds, distributed_directory_monitor_max_sleep_time_ms, 30000, "Maximum sleep time for StorageDistributed DirectoryMonitors, it limits exponential growth too.", 0) \
    \
    M(Bool, distributed_directory_monitor_batch_inserts, false, "Should StorageDistributed DirectoryMonitors try to batch individual inserts into bigger ones.", 0) \
    \
    M(Bool, optimize_move_to_prewhere, true, "Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree.", 0) \
    \
    M(Milliseconds, insert_in_memory_parts_timeout, 600000, "", 0) \
    \
    M(UInt64, replication_alter_partitions_sync, 1, "Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.", 0) \
    M(UInt64, replication_alter_columns_timeout, 60, "Wait for actions to change the table structure within the specified number of seconds. 0 - wait unlimited time.", 0) \
    \
    M(LoadBalancing, load_balancing, LoadBalancing::RANDOM, "Which replicas (among healthy replicas) to preferably send a query to (on the first attempt) for distributed processing.", 0) \
    \
    M(TotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE, "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.", IMPORTANT) \
    M(Float, totals_auto_threshold, 0.5, "The threshold for totals_mode = 'auto'.", 0) \
    \
    M(Bool, allow_suspicious_low_cardinality_types, false, "In CREATE TABLE statement allows specifying LowCardinality modifier for types of small fixed size (8 or less). Enabling this may increase merge times and memory consumption.", 0) \
    M(Bool, compile_expressions, false, "Compile some scalar functions and operators to native code.", 0) \
    M(UInt64, min_count_to_compile_expression, 3, "The number of identical expressions before they are JIT-compiled", 0) \
    M(UInt64, group_by_two_level_threshold, 100000, "From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.", 0) \
    M(UInt64, group_by_two_level_threshold_bytes, 100000000, "From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.", 0) \
    M(Bool, distributed_aggregation_memory_efficient, false, "Is the memory-saving mode of distributed aggregation enabled.", 0) \
    M(UInt64, aggregation_memory_efficient_merge_threads, 0, "Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as 'max_threads'.", 0) \
    \
    M(UInt64, max_parallel_replicas, 1, "The maximum number of replicas of each shard used when the query is executed. For consistency (to get different parts of the same partition), this option only works for the specified sampling key. The lag of the replicas is not controlled.", 0) \
    M(UInt64, parallel_replicas_count, 0, "", 0) \
    M(UInt64, parallel_replica_offset, 0, "", 0) \
    \
    M(SpecialSort, special_sort, SpecialSort::NOT_SPECIFIED, "Specifies a sorting algorithm which will be using in ORDER BY query.", 0) \
    \
    M(Bool, skip_unavailable_shards, false, "If 1, ClickHouse silently skips unavailable shards and nodes unresolvable through DNS. Shard is marked as unavailable when none of the replicas can be reached.", 0) \
    \
    M(Bool, distributed_group_by_no_merge, false, "Do not merge aggregation states from different servers for distributed query processing - in case it is for certain that there are different keys on different shards.", 0) \
    M(Bool, parallel_distributed_insert_select, false, "If true, distributed insert select query in the same cluster will be processed on local tables on every shard", 0) \
    M(Bool, optimize_distributed_group_by_sharding_key, false, "Optimize GROUP BY sharding_key queries (by avodiing costly aggregation on the initiator server).", 0) \
    M(Bool, optimize_skip_unused_shards, false, "Assumes that data is distributed by sharding_key. Optimization to skip unused shards if SELECT query filters by sharding_key.", 0) \
    M(UInt64, force_optimize_skip_unused_shards, 0, "Throw an exception if unused shards cannot be skipped (1 - throw only if the table has the sharding key, 2 - always throw.", 0) \
    M(UInt64, optimize_skip_unused_shards_nesting, 0, "Same as optimize_skip_unused_shards, but accept nesting level until which it will work.", 0) \
    M(UInt64, force_optimize_skip_unused_shards_nesting, 0, "Same as force_optimize_skip_unused_shards, but accept nesting level until which it will work.", 0) \
    \
    M(Bool, input_format_parallel_parsing, true, "Enable parallel parsing for some data formats.", 0) \
    M(UInt64, min_chunk_bytes_for_parallel_parsing, (10 * 1024 * 1024), "The minimum chunk size in bytes, which each thread will parse in parallel.", 0) \
    \
    M(UInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192), "If at least as many lines are read from one file, the reading can be parallelized.", 0) \
    M(UInt64, merge_tree_min_bytes_for_concurrent_read, (24 * 10 * 1024 * 1024), "If at least as many bytes are read from one file, the reading can be parallelized.", 0) \
    M(UInt64, merge_tree_min_rows_for_seek, 0, "You can skip reading more than that number of rows at the price of one seek per file.", 0) \
    M(UInt64, merge_tree_min_bytes_for_seek, 0, "You can skip reading more than that number of bytes at the price of one seek per file.", 0) \
    M(UInt64, merge_tree_coarse_index_granularity, 8, "If the index segment can contain the required keys, divide it into as many parts and recursively check them.", 0) \
    M(UInt64, merge_tree_max_rows_to_use_cache, (128 * 8192), "The maximum number of rows per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)", 0) \
    M(UInt64, merge_tree_max_bytes_to_use_cache, (192 * 10 * 1024 * 1024), "The maximum number of bytes per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)", 0) \
    \
    M(UInt64, mysql_max_rows_to_insert, 65536, "The maximum number of rows in MySQL batch insertion of the MySQL storage engine", 0) \
    \
    M(UInt64, optimize_min_equality_disjunction_chain_length, 3, "The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization ", 0) \
    \
    M(UInt64, min_bytes_to_use_direct_io, 0, "The minimum number of bytes for reading the data with O_DIRECT option during SELECT queries execution. 0 - disabled.", 0) \
    M(UInt64, min_bytes_to_use_mmap_io, 0, "The minimum number of bytes for reading the data with mmap option during SELECT queries execution. 0 - disabled.", 0) \
    \
    M(Bool, force_index_by_date, 0, "Throw an exception if there is a partition key in a table, and it is not used.", 0) \
    M(Bool, force_primary_key, 0, "Throw an exception if there is primary key in a table, and it is not used.", 0) \
    \
    M(Float, max_streams_to_max_threads_ratio, 1, "Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution, since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.", 0) \
    M(Float, max_streams_multiplier_for_merge_tables, 5, "Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and especially helpful when merged tables differ in size.", 0) \
    \
    M(String, network_compression_method, "LZ4", "Allows you to select the method of data compression when writing.", 0) \
    \
    M(Int64, network_zstd_compression_level, 1, "Allows you to select the level of ZSTD compression.", 0) \
    \
    M(UInt64, priority, 0, "Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.", 0) \
    M(Int64, os_thread_priority, 0, "If non zero - set corresponding 'nice' value for query processing threads. Can be used to adjust query priority for OS scheduler.", 0) \
    \
    M(Bool, log_queries, 1, "Log requests and write the log to the system table.", 0) \
    M(LogQueriesType, log_queries_min_type, QueryLogElementType::QUERY_START, "Minimal type in query_log to log, possible values (from low to high): QUERY_START, QUERY_FINISH, EXCEPTION_BEFORE_START, EXCEPTION_WHILE_PROCESSING.", 0) \
    M(UInt64, log_queries_cut_to_length, 100000, "If query length is greater than specified threshold (in bytes), then cut query when writing to query log. Also limit length of printed query in ordinary text log.", 0) \
    \
    M(DistributedProductMode, distributed_product_mode, DistributedProductMode::DENY, "How are distributed subqueries performed inside IN or JOIN sections?", IMPORTANT) \
    \
    M(UInt64, max_concurrent_queries_for_user, 0, "The maximum number of concurrent requests per user.", 0) \
    \
    M(Bool, insert_deduplicate, true, "For INSERT queries in the replicated table, specifies that deduplication of insertings blocks should be preformed", 0) \
    \
    M(UInt64, insert_quorum, 0, "For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled.", 0) \
    M(Milliseconds, insert_quorum_timeout, 600000, "", 0) \
    M(UInt64, select_sequential_consistency, 0, "For SELECT queries from the replicated table, throw an exception if the replica does not have a chunk written with the quorum; do not read the parts that have not yet been written with the quorum.", 0) \
    M(UInt64, table_function_remote_max_addresses, 1000, "The maximum number of different shards and the maximum number of replicas of one shard in the `remote` function.", 0) \
    M(Milliseconds, read_backoff_min_latency_ms, 1000, "Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.", 0) \
    M(UInt64, read_backoff_max_throughput, 1048576, "Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.", 0) \
    M(Milliseconds, read_backoff_min_interval_between_events_ms, 1000, "Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.", 0) \
    M(UInt64, read_backoff_min_events, 2, "Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.", 0) \
    \
    M(Float, memory_tracker_fault_probability, 0., "For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.", 0) \
    \
    M(Bool, enable_http_compression, 0, "Compress the result if the client over HTTP said that it understands data compressed by gzip or deflate.", 0) \
    M(Int64, http_zlib_compression_level, 3, "Compression level - used if the client on HTTP said that it understands data compressed by gzip or deflate.", 0) \
    \
    M(Bool, http_native_compression_disable_checksumming_on_decompress, 0, "If you uncompress the POST data from the client compressed by the native format, do not check the checksum.", 0) \
    \
    M(String, count_distinct_implementation, "uniqExact", "What aggregate function to use for implementation of count(DISTINCT ...)", 0) \
    \
    M(Bool, add_http_cors_header, false, "Write add http CORS header.", 0) \
    \
    M(UInt64, max_http_get_redirects, 0, "Max number of http GET redirects hops allowed. Make sure additional security measures are in place to prevent a malicious server to redirect your requests to unexpected services.", 0) \
    \
    M(Bool, use_client_time_zone, false, "Use client timezone for interpreting DateTime string values, instead of adopting server timezone.", 0) \
    \
    M(Bool, send_progress_in_http_headers, false, "Send progress notifications using X-ClickHouse-Progress headers. Some clients do not support high amount of HTTP headers (Python requests in particular), so it is disabled by default.", 0) \
    \
    M(UInt64, http_headers_progress_interval_ms, 100, "Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.", 0) \
    \
    M(Bool, fsync_metadata, 1, "Do fsync after changing metadata for tables and databases (.sql files). Could be disabled in case of poor latency on server with high load of DDL queries and high load of disk subsystem.", 0) \
    \
    M(Bool, join_use_nulls, 0, "Use NULLs for non-joined rows of outer JOINs for types that can be inside Nullable. If false, use default value of corresponding columns data type.", IMPORTANT) \
    \
    M(JoinStrictness, join_default_strictness, JoinStrictness::ALL, "Set default strictness in JOIN query. Possible values: empty string, 'ANY', 'ALL'. If empty, query without strictness will throw exception.", 0) \
    M(Bool, any_join_distinct_right_table_keys, false, "Enable old ANY JOIN logic with many-to-one left-to-right table keys mapping for all ANY JOINs. It leads to confusing not equal results for 't1 ANY LEFT JOIN t2' and 't2 ANY RIGHT JOIN t1'. ANY RIGHT JOIN needs one-to-many keys mapping to be consistent with LEFT one.", IMPORTANT) \
    \
    M(UInt64, preferred_block_size_bytes, 1000000, "", 0) \
    \
    M(UInt64, max_replica_delay_for_distributed_queries, 300, "If set, distributed queries of Replicated tables will choose servers with replication delay in seconds less than the specified value (not inclusive). Zero means do not take delay into account.", 0) \
    M(Bool, fallback_to_stale_replicas_for_distributed_queries, 1, "Suppose max_replica_delay_for_distributed_queries is set and all replicas for the queried table are stale. If this setting is enabled, the query will be performed anyway, otherwise the error will be reported.", 0) \
    M(UInt64, preferred_max_column_in_block_size_bytes, 0, "Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.", 0) \
    \
    M(Bool, insert_distributed_sync, false, "If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster.", 0) \
    M(UInt64, insert_distributed_timeout, 0, "Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.", 0) \
    M(Int64, distributed_ddl_task_timeout, 180, "Timeout for DDL query responses from all hosts in cluster. If a ddl request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite.", 0) \
    M(Milliseconds, stream_flush_interval_ms, 7500, "Timeout for flushing data from streaming storages.", 0) \
    M(Milliseconds, stream_poll_timeout_ms, 500, "Timeout for polling data from/to streaming storages.", 0) \
    \
    M(Bool, insert_allow_materialized_columns, 0, "If setting is enabled, Allow materialized columns in INSERT.", 0) \
    M(Seconds, http_connection_timeout, DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, "HTTP connection timeout.", 0) \
    M(Seconds, http_send_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, "HTTP send timeout", 0) \
    M(Seconds, http_receive_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, "HTTP receive timeout", 0) \
    M(Bool, optimize_throw_if_noop, false, "If setting is enabled and OPTIMIZE query didn't actually assign a merge then an explanatory exception is thrown", 0) \
    M(Bool, use_index_for_in_with_subqueries, true, "Try using an index if there is a subquery or a table expression on the right side of the IN operator.", 0) \
    M(Bool, joined_subquery_requires_alias, true, "Force joined subqueries and table functions to have aliases for correct name qualification.", 0) \
    M(Bool, empty_result_for_aggregation_by_empty_set, false, "Return empty result when aggregating without keys on empty set.", 0) \
    M(Bool, allow_distributed_ddl, true, "If it is set to true, then a user is allowed to executed distributed DDL queries.", 0) \
    M(Bool, allow_suspicious_codecs, false, "If it is set to true, allow to specify meaningless compression codecs.", 0) \
    M(UInt64, odbc_max_field_size, 1024, "Max size of filed can be read from ODBC dictionary. Long strings are truncated.", 0) \
    M(UInt64, query_profiler_real_time_period_ns, 1000000000, "Period for real clock timer of query profiler (in nanoseconds). Set 0 value to turn off the real clock query profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", 0) \
    M(UInt64, query_profiler_cpu_time_period_ns, 1000000000, "Period for CPU clock timer of query profiler (in nanoseconds). Set 0 value to turn off the CPU clock query profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", 0) \
    M(Bool, metrics_perf_events_enabled, false, "If enabled, some of the perf events will be measured throughout queries' execution.", 0) \
    M(String, metrics_perf_events_list, "", "Comma separated list of perf metrics that will be measured throughout queries' execution. Empty means all events. See PerfEventInfo in sources for the available events.", 0) \
    \
    \
    /** Limits during query execution are part of the settings. \
      * Used to provide a more safe execution of queries from the user interface. \
      * Basically, limits are checked for each block (not every row). That is, the limits can be slightly violated. \
      * Almost all limits apply only to SELECTs. \
      * Almost all limits apply to each stream individually. \
      */ \
    \
    M(UInt64, max_rows_to_read, 0, "Limit on read rows from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.", 0) \
    M(UInt64, max_bytes_to_read, 0, "Limit on read bytes (after decompression) from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.", 0) \
    M(OverflowMode, read_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_to_group_by, 0, "", 0) \
    M(OverflowModeGroupBy, group_by_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, max_bytes_before_external_group_by, 0, "", 0) \
    \
    M(UInt64, max_rows_to_sort, 0, "", 0) \
    M(UInt64, max_bytes_to_sort, 0, "", 0) \
    M(OverflowMode, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, max_bytes_before_external_sort, 0, "", 0) \
    M(UInt64, max_bytes_before_remerge_sort, 1000000000, "In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.", 0) \
    \
    M(UInt64, max_result_rows, 0, "Limit on result size in rows. Also checked for intermediate data sent from remote servers.", 0) \
    M(UInt64, max_result_bytes, 0, "Limit on result size in bytes (uncompressed). Also checked for intermediate data sent from remote servers.", 0) \
    M(OverflowMode, result_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    /* TODO: Check also when merging and finalizing aggregate functions. */ \
    M(Seconds, max_execution_time, 0, "", 0) \
    M(OverflowMode, timeout_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, min_execution_speed, 0, "Minimum number of execution rows per second.", 0) \
    M(UInt64, max_execution_speed, 0, "Maximum number of execution rows per second.", 0) \
    M(UInt64, min_execution_speed_bytes, 0, "Minimum number of execution bytes per second.", 0) \
    M(UInt64, max_execution_speed_bytes, 0, "Maximum number of execution bytes per second.", 0) \
    M(Seconds, timeout_before_checking_execution_speed, 10, "Check that the speed is not too low after the specified time has elapsed.", 0) \
    \
    M(UInt64, max_columns_to_read, 0, "", 0) \
    M(UInt64, max_temporary_columns, 0, "", 0) \
    M(UInt64, max_temporary_non_const_columns, 0, "", 0) \
    \
    M(UInt64, max_subquery_depth, 100, "", 0) \
    M(UInt64, max_pipeline_depth, 1000, "", 0) \
    M(UInt64, max_ast_depth, 1000, "Maximum depth of query syntax tree. Checked after parsing.", 0) \
    M(UInt64, max_ast_elements, 50000, "Maximum size of query syntax tree in number of nodes. Checked after parsing.", 0) \
    M(UInt64, max_expanded_ast_elements, 500000, "Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.", 0) \
    \
    M(UInt64, readonly, 0, "0 - everything is allowed. 1 - only read requests. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.", 0) \
    \
    M(UInt64, max_rows_in_set, 0, "Maximum size of the set (in number of elements) resulting from the execution of the IN section.", 0) \
    M(UInt64, max_bytes_in_set, 0, "Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.", 0) \
    M(OverflowMode, set_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_in_join, 0, "Maximum size of the hash table for JOIN (in number of rows).", 0) \
    M(UInt64, max_bytes_in_join, 0, "Maximum size of the hash table for JOIN (in number of bytes in memory).", 0) \
    M(OverflowMode, join_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(Bool, join_any_take_last_row, false, "When disabled (default) ANY JOIN will take the first found row for a key. When enabled, it will take the last row seen if there are multiple rows for the same key.", IMPORTANT) \
    M(JoinAlgorithm, join_algorithm, JoinAlgorithm::HASH, "Specify join algorithm: 'auto', 'hash', 'partial_merge', 'prefer_partial_merge'. 'auto' tries to change HashJoin to MergeJoin on the fly to avoid out of memory.", 0) \
    M(Bool, partial_merge_join_optimizations, true, "Enable optimizations in partial merge join", 0) \
    M(UInt64, default_max_bytes_in_join, 1000000000, "Maximum size of right-side table if limit is required but max_bytes_in_join is not set.", 0) \
    M(UInt64, partial_merge_join_left_table_buffer_bytes, 32000000, "If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread. In current version work only with 'partial_merge_join_optimizations = 1'.", 0) \
    M(UInt64, partial_merge_join_rows_in_right_blocks, 65536, "Split right-hand joining data in blocks of specified size. It's a portion of data indexed by min-max values and possibly unloaded on disk.", 0) \
    M(UInt64, join_on_disk_max_files_to_merge, 64, "For MergeJoin on disk set how much files it's allowed to sort simultaneously. Then this value bigger then more memory used and then less disk I/O needed. Minimum is 2.", 0) \
    M(String, temporary_files_codec, "LZ4", "Set compression codec for temporary files (sort and join on disk). I.e. LZ4, NONE.", 0) \
    \
    M(UInt64, max_rows_to_transfer, 0, "Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.", 0) \
    M(UInt64, max_bytes_to_transfer, 0, "Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.", 0) \
    M(OverflowMode, transfer_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.", 0) \
    M(UInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.", 0) \
    M(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_memory_usage, 0, "Maximum memory usage for processing of single query. Zero means unlimited.", 0) \
    M(UInt64, max_memory_usage_for_user, 0, "Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.", 0) \
    M(UInt64, max_untracked_memory, (4 * 1024 * 1024), "Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when amount (in absolute value) becomes larger than specified value. If the value is higher than 'memory_profiler_step' it will be effectively lowered to 'memory_profiler_step'.", 0) \
    M(UInt64, memory_profiler_step, 0, "Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stack trace. Zero means disabled memory profiler. Values lower than a few megabytes will slow down query processing.", 0) \
    M(Float, memory_profiler_sample_probability, 0., "Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability is for every alloc/free regardless to the size of the allocation. Note that sampling happens only when the amount of untracked memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine grained sampling.", 0) \
    \
    M(UInt64, max_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for a query. Zero means unlimited.", 0) \
    M(UInt64, max_network_bytes, 0, "The maximum number of bytes (compressed) to receive or transmit over the network for execution of the query.", 0) \
    M(UInt64, max_network_bandwidth_for_user, 0, "The maximum speed of data exchange over the network in bytes per second for all concurrently running user queries. Zero means unlimited.", 0)\
    M(UInt64, max_network_bandwidth_for_all_users, 0, "The maximum speed of data exchange over the network in bytes per second for all concurrently running queries. Zero means unlimited.", 0) \
    \
    M(Bool, log_profile_events, true, "Log query performance statistics into the query_log and query_thread_log.", 0) \
    M(Bool, log_query_settings, true, "Log query settings into the query_log.", 0) \
    M(Bool, log_query_threads, true, "Log query threads into system.query_thread_log table. This setting have effect only when 'log_queries' is true.", 0) \
    M(LogsLevel, send_logs_level, LogsLevel::fatal, "Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(Bool, enable_optimize_predicate_expression, 1, "If it is set to true, optimize predicates to subqueries.", 0) \
    M(Bool, enable_optimize_predicate_expression_to_final_subquery, 1, "Allow push predicate to final subquery.", 0) \
    M(Bool, allow_push_predicate_when_subquery_contains_with, 1, "Allows push predicate when subquery contains WITH clause", 0) \
    \
    M(UInt64, low_cardinality_max_dictionary_size, 8192, "Maximum size (in rows) of shared global dictionary for LowCardinality type.", 0) \
    M(Bool, low_cardinality_use_single_dictionary_for_part, false, "LowCardinality type serialization setting. If is true, than will use additional keys when global dictionary overflows. Otherwise, will create several shared dictionaries.", 0) \
    M(Bool, decimal_check_overflow, true, "Check overflow of decimal arithmetic/comparison operations", 0) \
    \
    M(Bool, prefer_localhost_replica, 1, "1 - always send query to local replica, if it exists. 0 - choose replica to send query between local and remote ones according to load_balancing", 0) \
    M(UInt64, max_fetch_partition_retries_count, 5, "Amount of retries while fetching partition from another host.", 0) \
    M(UInt64, http_max_multipart_form_data_size, 1024 * 1024 * 1024, "Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in user profile. Note that content is parsed and external tables are created in memory before start of query execution. And this is the only limit that has effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).", 0) \
    M(Bool, calculate_text_stack_trace, 1, "Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when huge amount of wrong queries are executed. In normal cases you should not disable this option.", 0) \
    M(Bool, allow_ddl, true, "If it is set to true, then a user is allowed to executed DDL queries.", 0) \
    M(Bool, parallel_view_processing, false, "Enables pushing to attached views concurrently instead of sequentially.", 0) \
    M(Bool, enable_debug_queries, false, "Enables debug queries such as AST.", 0) \
    M(Bool, enable_unaligned_array_join, false, "Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.", 0) \
    M(Bool, optimize_read_in_order, true, "Enable ORDER BY optimization for reading data in corresponding order in MergeTree tables.", 0) \
    M(Bool, optimize_aggregation_in_order, false, "Enable GROUP BY optimization for aggregating data in corresponding order in MergeTree tables.", 0) \
    M(UInt64, read_in_order_two_level_merge_threshold, 100, "Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.", 0) \
    M(Bool, low_cardinality_allow_in_native_format, true, "Use LowCardinality type in Native format. Otherwise, convert LowCardinality columns to ordinary for select query, and convert ordinary columns to required LowCardinality for insert query.", 0) \
    M(Bool, cancel_http_readonly_queries_on_client_close, false, "Cancel HTTP readonly queries when a client closes the connection without waiting for response.", 0) \
    M(Bool, external_table_functions_use_nulls, true, "If it is set to true, external table functions will implicitly use Nullable type if needed. Otherwise NULLs will be substituted with default values. Currently supported only by 'mysql' and 'odbc' table functions.", 0) \
    \
    M(Bool, allow_hyperscan, true, "Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.", 0) \
    M(Bool, allow_simdjson, true, "Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.", 0) \
    M(Bool, allow_introspection_functions, false, "Allow functions for introspection of ELF and DWARF for query profiling. These functions are slow and may impose security considerations.", 0) \
    \
    M(UInt64, max_partitions_per_insert_block, 100, "Limit maximum number of partitions in single INSERTed block. Zero means unlimited. Throw exception if the block contains too many partitions. This setting is a safety threshold, because using large number of partitions is a common misconception.", 0) \
    M(Bool, check_query_single_value_result, true, "Return check query result as single 1/0 value", 0) \
    M(Bool, allow_drop_detached, false, "Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries", 0) \
    \
    M(Seconds, distributed_replica_error_half_life, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD, "Time period reduces replica error counter by 2 times.", 0) \
    M(UInt64, distributed_replica_error_cap, DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT, "Max number of errors per replica, prevents piling up an incredible amount of errors if replica was offline for some time and allows it to be reconsidered in a shorter amount of time.", 0) \
    M(UInt64, distributed_replica_max_ignored_errors, 0, "Number of errors that will be ignored while choosing replicas", 0) \
    \
    M(Bool, allow_experimental_live_view, false, "Enable LIVE VIEW. Not mature enough.", 0) \
    M(Seconds, live_view_heartbeat_interval, DEFAULT_LIVE_VIEW_HEARTBEAT_INTERVAL_SEC, "The heartbeat interval in seconds to indicate live query is alive.", 0) \
    M(UInt64, max_live_view_insert_blocks_before_refresh, 64, "Limit maximum number of inserted blocks after which mergeable blocks are dropped and query is re-executed.", 0) \
    M(UInt64, min_free_disk_space_for_temporary_data, 0, "The minimum disk space to keep while writing temporary data used in external sorting and aggregation.", 0) \
    \
    M(DefaultDatabaseEngine, default_database_engine, DefaultDatabaseEngine::Ordinary, "Default database engine.", 0) \
    M(Bool, allow_experimental_database_atomic, false, "Allow to create database with Engine=Atomic.", 0) \
    M(Bool, show_table_uuid_in_table_create_query_if_not_nil, true, "For tables in databases with Engine=Atomic show UUID of the table in its CREATE query.", 0) \
    M(Bool, enable_scalar_subquery_optimization, true, "If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.", 0) \
    M(Bool, optimize_trivial_count_query, true, "Process trivial 'SELECT count() FROM table' query from metadata.", 0) \
    M(UInt64, mutations_sync, 0, "Wait for synchronous execution of ALTER TABLE UPDATE/DELETE queries (mutations). 0 - execute asynchronously. 1 - wait current server. 2 - wait all replicas if they exist.", 0) \
    M(Bool, optimize_move_functions_out_of_any, true, "Move functions out of aggregate functions 'any', 'anyLast'.", 0) \
    M(Bool, optimize_injective_functions_inside_uniq, true, "Delete injective functions of one argument inside uniq*() functions.", 0) \
    M(Bool, optimize_arithmetic_operations_in_aggregate_functions, true, "Move arithmetic operations out of aggregation functions", 0) \
    M(Bool, optimize_duplicate_order_by_and_distinct, true, "Remove duplicate ORDER BY and DISTINCT if it's possible", 0) \
    M(Bool, optimize_redundant_functions_in_order_by, true, "Remove functions from ORDER BY if its argument is also in ORDER BY", 0) \
    M(Bool, optimize_if_chain_to_multiif, false, "Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not beneficial for numeric types.", 0) \
    M(Bool, optimize_if_transform_strings_to_enum, false, "Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in distributed query that would lead to its fail.", 0) \
    M(Bool, optimize_monotonous_functions_in_order_by, true, "Replace monotonous function with its argument in ORDER BY", 0) \
    M(Bool, allow_experimental_alter_materialized_view_structure, false, "Allow atomic alter on Materialized views. Work in progress.", 0) \
    M(Bool, enable_early_constant_folding, true, "Enable query optimization where we analyze function and subqueries results and rewrite query if there're constants there", 0) \
    \
    M(Bool, deduplicate_blocks_in_dependent_materialized_views, false, "Should deduplicate blocks for materialized views if the block is not a duplicate for the table. Use true to always deduplicate in dependent tables.", 0) \
    M(Bool, use_compact_format_in_distributed_parts_names, false, "Changes format of directories names for distributed table insert parts.", 0) \
    M(UInt64, multiple_joins_rewriter_version, 2, "1 or 2. Second rewriter version knows about table columns and keep not clashed names as is.", 0) \
    M(Bool, validate_polygons, true, "Throw exception if polygon is invalid in function pointInPolygon (e.g. self-tangent, self-intersecting). If the setting is false, the function will accept invalid polygons but may silently return wrong result.", 0) \
    M(UInt64, max_parser_depth, DBMS_DEFAULT_MAX_PARSER_DEPTH, "Maximum parser depth (recursion depth of recursive descend parser).", 0) \
    M(Seconds, temporary_live_view_timeout, DEFAULT_TEMPORARY_LIVE_VIEW_TIMEOUT_SEC, "Timeout after which temporary live view is deleted.", 0) \
    M(Bool, transform_null_in, false, "If enabled, NULL values will be matched with 'IN' operator as if they are considered equal.", 0) \
    M(Bool, allow_nondeterministic_mutations, false, "Allow non-deterministic functions in ALTER UPDATE/ALTER DELETE statements", 0) \
    M(Seconds, lock_acquire_timeout, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "How long locking request should wait before failing", 0) \
    M(Bool, materialize_ttl_after_modify, true, "Apply TTL for old data, after ALTER MODIFY TTL query", 0) \
    M(String, function_implementation, "", "Choose function implementation for specific target or variant (experimental). If empty enable all of them.", 0) \
    \
    M(Bool, allow_experimental_geo_types, false, "Allow geo data types such as Point, Ring, Polygon, MultiPolygon", 0) \
    M(Bool, data_type_default_nullable, false, "Data types without NULL or NOT NULL will make Nullable", 0) \
    M(Bool, cast_keep_nullable, false, "CAST operator keep Nullable for result data type", 0) \
    M(Bool, alter_partition_verbose_result, false, "Output information about affected parts. Currently works only for FREEZE and ATTACH commands.", 0) \
    \
    /** Obsolete settings that do nothing but left for compatibility reasons. Remove each one after half a year of obsolescence. */ \
    \
    M(Bool, allow_experimental_low_cardinality_type, true, "Obsolete setting, does nothing. Will be removed after 2019-08-13", 0) \
    M(Bool, compile, false, "Whether query compilation is enabled. Will be removed after 2020-03-13", 0) \
    M(UInt64, min_count_to_compile, 0, "Obsolete setting, does nothing. Will be removed after 2020-03-13", 0) \
    M(Bool, allow_experimental_multiple_joins_emulation, true, "Obsolete setting, does nothing. Will be removed after 2020-05-31", 0) \
    M(Bool, allow_experimental_cross_to_join_conversion, true, "Obsolete setting, does nothing. Will be removed after 2020-05-31", 0) \
    M(Bool, allow_experimental_data_skipping_indices, true, "Obsolete setting, does nothing. Will be removed after 2020-05-31", 0) \
    M(Bool, merge_tree_uniform_read_distribution, true, "Obsolete setting, does nothing. Will be removed after 2020-05-20", 0) \
    M(UInt64, mark_cache_min_lifetime, 0, "Obsolete setting, does nothing. Will be removed after 2020-05-31", 0) \
    M(Bool, partial_merge_join, false, "Obsolete. Use join_algorithm='prefer_partial_merge' instead.", 0) \
    M(UInt64, max_memory_usage_for_all_queries, 0, "Obsolete. Will be removed after 2020-10-20", 0) \
    \
    M(Bool, force_optimize_skip_unused_shards_no_nested, false, "Obsolete setting, does nothing. Will be removed after 2020-12-01. Use force_optimize_skip_unused_shards_nesting instead.", 0) \
    M(Bool, experimental_use_processors, true, "Obsolete setting, does nothing. Will be removed after 2020-11-29.", 0)

#define FORMAT_FACTORY_SETTINGS(M) \
    M(Char, format_csv_delimiter, ',', "The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.", 0) \
    M(Bool, format_csv_allow_single_quotes, 1, "If it is set to true, allow strings in single quotes.", 0) \
    M(Bool, format_csv_allow_double_quotes, 1, "If it is set to true, allow strings in double quotes.", 0) \
    M(Bool, output_format_csv_crlf_end_of_line, false, "If it is set true, end of line in CSV format will be \\r\\n instead of \\n.", 0) \
    M(Bool, input_format_csv_unquoted_null_literal_as_null, false, "Consider unquoted NULL literal as \\N", 0) \
    M(Bool, input_format_skip_unknown_fields, false, "Skip columns with unknown names from input data (it works for JSONEachRow, CSVWithNames, TSVWithNames and TSKV formats).", 0) \
    M(Bool, input_format_with_names_use_header, true, "For TSVWithNames and CSVWithNames input formats this controls whether format parser is to assume that column data appear in the input exactly as they are specified in the header.", 0) \
    M(Bool, input_format_import_nested_json, false, "Map nested JSON data to nested tables (it works for JSONEachRow format).", 0) \
    M(Bool, optimize_aggregators_of_group_by_keys, true, "Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section", 0) \
    M(Bool, input_format_defaults_for_omitted_fields, true, "For input data calculate default expressions for omitted fields (it works for JSONEachRow, CSV and TSV formats).", IMPORTANT) \
    M(Bool, input_format_tsv_empty_as_default, false, "Treat empty fields in TSV input as default values.", 0) \
    M(Bool, input_format_null_as_default, false, "For text input formats initialize null fields with default values if data type of this field is not nullable", 0) \
    \
    M(DateTimeInputFormat, date_time_input_format, FormatSettings::DateTimeInputFormat::Basic, "Method to read DateTime from text input formats. Possible values: 'basic' and 'best_effort'.", 0) \
    \
    M(Bool, optimize_group_by_function_keys, true, "Eliminates functions of other keys in GROUP BY section", 0) \
    M(Bool, input_format_values_interpret_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.", 0) \
    M(Bool, input_format_values_deduce_templates_of_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.", 0) \
    M(Bool, input_format_values_accurate_types_of_literals, true, "For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.", 0) \
    M(Bool, input_format_avro_allow_missing_fields, false, "For Avro/AvroConfluent format: when field is not found in schema use default value instead of error", 0) \
    M(URI, format_avro_schema_registry_url, "", "For AvroConfluent format: Confluent Schema Registry URL.", 0) \
    \
    M(Bool, output_format_json_quote_64bit_integers, true, "Controls quoting of 64-bit integers in JSON output format.", 0) \
    \
    M(Bool, output_format_json_quote_denormals, false, "Enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.", 0) \
    \
    M(Bool, output_format_json_escape_forward_slashes, true, "Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don't confuse with backslashes that are always escaped.", 0) \
    \
    M(UInt64, output_format_pretty_max_rows, 10000, "Rows limit for Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_column_pad_width, 250, "Maximum width to pad all values in a column in Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_value_width, 10000, "Maximum width of value to display in Pretty formats. If greater - it will be cut.", 0) \
    M(Bool, output_format_pretty_color, true, "Use ANSI escape sequences to paint colors in Pretty formats", 0) \
    M(String, output_format_pretty_grid_charset, "UTF-8", "Charset for printing grid borders. Available charsets: ASCII, UTF-8 (default one).", 0) \
    M(UInt64, output_format_parquet_row_group_size, 1000000, "Row group size in rows.", 0) \
    M(String, output_format_avro_codec, "", "Compression codec used for output. Possible values: 'null', 'deflate', 'snappy'.", 0) \
    M(UInt64, output_format_avro_sync_interval, 16 * 1024, "Sync interval in bytes.", 0) \
    M(Bool, output_format_tsv_crlf_end_of_line, false, "If it is set true, end of line in TSV format will be \\r\\n instead of \\n.", 0) \
    \
    M(UInt64, input_format_allow_errors_num, 0, "Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    M(Float, input_format_allow_errors_ratio, 0, "Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    \
    M(String, format_schema, "", "Schema identifier (used by schema-based formats)", 0) \
    M(String, format_template_resultset, "", "Path to file which contains format string for result set (for Template format)", 0) \
    M(String, format_template_row, "", "Path to file which contains format string for rows (for Template format)", 0) \
    M(String, format_template_rows_between_delimiter, "\n", "Delimiter between rows (for Template format)", 0) \
    \
    M(String, format_custom_escaping_rule, "Escaped", "Field escaping rule (for CustomSeparated format)", 0) \
    M(String, format_custom_field_delimiter, "\t", "Delimiter between fields (for CustomSeparated format)", 0) \
    M(String, format_custom_row_before_delimiter, "", "Delimiter before field of the first column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_after_delimiter, "\n", "Delimiter after field of the last column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_between_delimiter, "", "Delimiter between rows (for CustomSeparated format)", 0) \
    M(String, format_custom_result_before_delimiter, "", "Prefix before result set (for CustomSeparated format)", 0) \
    M(String, format_custom_result_after_delimiter, "", "Suffix after result set (for CustomSeparated format)", 0) \
    \
    M(String, format_regexp, "", "Regular expression (for Regexp format)", 0) \
    M(String, format_regexp_escaping_rule, "Escaped", "Field escaping rule (for Regexp format)", 0) \
    M(Bool, format_regexp_skip_unmatched, false, "Skip lines unmatched by regular expression (for Regexp format", 0) \
    \
    M(Bool, output_format_enable_streaming, false, "Enable streaming in output formats that support it.", 0) \
    M(Bool, output_format_write_statistics, true, "Write statistics about read rows, bytes, time elapsed in suitable output formats.", 0) \
    M(Bool, allow_non_metadata_alters, true, "Allow to execute alters which affects not only tables metadata, but also data on disk", 0) \

#define LIST_OF_SETTINGS(M)    \
    COMMON_SETTINGS(M)         \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(SettingsTraits, LIST_OF_SETTINGS)


/** Settings of query execution.
  * These settings go to users.xml.
  */
struct Settings : public BaseSettings<SettingsTraits>
{
    /// For initialization from empty initializer-list to be "value initialization", not "aggregate initialization" in C++14.
    /// http://en.cppreference.com/w/cpp/language/aggregate_initialization
    Settings() {}

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
     * The profile can also be set using the `set` functions, like the profile setting.
     */
    void setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config);

    /// Load settings from configuration file, at "path" prefix in configuration.
    void loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

    /// Dumps profile events to two columns of type Array(String)
    void dumpToArrayColumns(IColumn * column_names, IColumn * column_values, bool changed_only = true);

    /// Adds program options to set the settings from a command line.
    /// (Don't forget to call notify() on the `variables_map` after parsing it!)
    void addProgramOptions(boost::program_options::options_description & options);
};

}
