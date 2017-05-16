#pragma once

#include <Poco/Timespan.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Core/Field.h>

#include <Interpreters/Limits.h>
#include <Interpreters/SettingsCommon.h>


namespace DB
{

/** Settings of query execution.
  */
struct Settings
{
    /// For initialization from empty initializer-list to be "value initialization", not "aggregate initialization" in С++14.
    /// http://en.cppreference.com/w/cpp/language/aggregate_initialization
    Settings() {}

    /** List of settings: type, name, default value.
      *
      * This looks rather unconvenient. It is done that way to avoid repeating settings in different places.
      * Note: as an alternative, we could implement settings to be completely dynamic in form of map: String -> Field,
      *  but we are not going to do it, because settings is used everywhere as static struct fields.
      */

#define APPLY_FOR_SETTINGS(M) \
    /** When writing data, a buffer of max_compress_block_size size is allocated for compression. When the buffer overflows or if into the buffer */ \
    /** written data is greater than or equal to min_compress_block_size, then with the next mark, the data will also be compressed */ \
    /** As a result, for small columns (around 1-8 bytes), with index_granularity = 8192, the block size will be 64 KB. */ \
    /** And for large columns (Title - string ~100 bytes), the block size will be ~819 KB. */ \
    /** Due to this, the compression ratio almost does not get worse. */ \
    M(SettingUInt64, min_compress_block_size, DEFAULT_MIN_COMPRESS_BLOCK_SIZE) \
    M(SettingUInt64, max_compress_block_size, DEFAULT_MAX_COMPRESS_BLOCK_SIZE) \
    /** Maximum block size for reading */ \
    M(SettingUInt64, max_block_size, DEFAULT_BLOCK_SIZE) \
    /** The maximum block size for insertion, if we control the creation of blocks for insertion. */ \
    M(SettingUInt64, max_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE) \
    /** Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough. */ \
    M(SettingUInt64, min_insert_block_size_rows, DEFAULT_INSERT_BLOCK_SIZE) \
    /** Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough. */ \
    M(SettingUInt64, min_insert_block_size_bytes, (DEFAULT_INSERT_BLOCK_SIZE * 256)) \
    /** The maximum number of threads to execute the request. By default, it is determined automatically. */ \
    M(SettingMaxThreads, max_threads, 0) \
    /** The maximum size of the buffer to read from the file system. */ \
    M(SettingUInt64, max_read_buffer_size, DBMS_DEFAULT_BUFFER_SIZE) \
    /** The maximum number of connections for distributed processing of one query (should be greater than max_threads). */ \
    M(SettingUInt64, max_distributed_connections, DEFAULT_MAX_DISTRIBUTED_CONNECTIONS) \
    /** Which part of the query can be read into RAM for parsing (the remaining data for INSERT, if any, is read later) */ \
    M(SettingUInt64, max_query_size, DEFAULT_MAX_QUERY_SIZE) \
    /** The interval in microseconds to check if the request is cancelled, and to send progress info. */ \
    M(SettingUInt64, interactive_delay, DEFAULT_INTERACTIVE_DELAY) \
    M(SettingSeconds, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC) \
    /** If you should select one of the working replicas. */ \
    M(SettingMilliseconds, connect_timeout_with_failover_ms, DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS) \
    M(SettingSeconds, receive_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC) \
    M(SettingSeconds, send_timeout, DBMS_DEFAULT_SEND_TIMEOUT_SEC) \
    /** The wait time in the request queue, if the number of concurrent requests exceeds the maximum. */ \
    M(SettingMilliseconds, queue_max_wait_ms, DEFAULT_QUERIES_QUEUE_WAIT_TIME_MS) \
    /** Block at the query wait cycle on the server for the specified number of seconds. */ \
    M(SettingUInt64, poll_interval, DBMS_DEFAULT_POLL_INTERVAL) \
    /** Maximum number of connections with one remote server in the pool. */ \
    M(SettingUInt64, distributed_connections_pool_size, DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE) \
    /** The maximum number of attempts to connect to replicas. */ \
    M(SettingUInt64, connections_with_failover_max_tries, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES) \
    /** Calculate minimums and maximums of the result columns. They can be output in JSON-formats. */ \
    M(SettingBool, extremes, false) \
    /** Whether to use the cache of uncompressed blocks. */ \
    M(SettingBool, use_uncompressed_cache, true) \
    /** Whether the running request should be canceled with the same id as the new one. */ \
    M(SettingBool, replace_running_query, false) \
    /** Number of threads performing background work for tables (for example, merging in merge tree). \
      * TODO: Now only applies when the server is started. You can make it dynamically variable. */ \
    M(SettingUInt64, background_pool_size, DBMS_DEFAULT_BACKGROUND_POOL_SIZE) \
    \
    /** Sleep time for StorageDistributed DirectoryMonitors in case there is no work or exception has been thrown */ \
    M(SettingMilliseconds, distributed_directory_monitor_sleep_time_ms, DBMS_DISTRIBUTED_DIRECTORY_MONITOR_SLEEP_TIME_MS) \
    \
    /** Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree */ \
    M(SettingBool, optimize_move_to_prewhere, true) \
    \
    /** Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone. */ \
    M(SettingUInt64, replication_alter_partitions_sync, 1) \
    /** Wait for actions to change the table structure within the specified number of seconds. 0 - wait unlimited time. */ \
    M(SettingUInt64, replication_alter_columns_timeout, 60) \
    \
    M(SettingLoadBalancing, load_balancing, LoadBalancing::RANDOM) \
    \
    M(SettingTotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE) \
    M(SettingFloat, totals_auto_threshold, 0.5) \
    \
    /** Whether query compilation is enabled. */ \
    M(SettingBool, compile, false) \
    /** The number of structurally identical queries before they are compiled. */ \
    M(SettingUInt64, min_count_to_compile, 3) \
    /** From what number of keys, a two-level aggregation starts. 0 - the threshold is not set. */ \
    M(SettingUInt64, group_by_two_level_threshold, 100000) \
    /** From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. \
      * Two-level aggregation is used when at least one of the thresholds is triggered. */ \
    M(SettingUInt64, group_by_two_level_threshold_bytes, 100000000) \
    /** Is the memory-saving mode of distributed aggregation enabled. */ \
    M(SettingBool, distributed_aggregation_memory_efficient, false) \
    /** Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. \
      * 0 means - same as 'max_threads'. */ \
    M(SettingUInt64, aggregation_memory_efficient_merge_threads, 0) \
    \
    /** The maximum number of replicas of each shard used when executing the query */ \
    M(SettingUInt64, max_parallel_replicas, 1) \
    M(SettingUInt64, parallel_replicas_count, 0) \
    M(SettingUInt64, parallel_replica_offset, 0) \
    \
    /** Silently skip unavailable shards. */ \
    M(SettingBool, skip_unavailable_shards, false) \
    \
    /** Do not merge aggregation states from different servers for distributed query processing \
      *  - in case it is for certain that there are different keys on different shards. \
      */ \
    M(SettingBool, distributed_group_by_no_merge, false) \
    \
    /** Advanced settings for reading from MergeTree */ \
    \
    /** If at least as many lines are read from one file, the reading can be parallelized. */ \
    M(SettingUInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192)) \
    /** You can skip reading more than that number of rows at the price of one seek per file. */ \
    M(SettingUInt64, merge_tree_min_rows_for_seek, 0) \
    /** If the index segment can contain the required keys, divide it into as many parts and recursively check them. */ \
    M(SettingUInt64, merge_tree_coarse_index_granularity, 8) \
    /** The maximum number of rows per request, to use the cache of uncompressed data. If the request is large, the cache is not used. \
      * (For large queries not to flush out the cache.) */ \
    M(SettingUInt64, merge_tree_max_rows_to_use_cache, (1024 * 1024)) \
    \
    /** Distribute read from MergeTree over threads evenly, ensuring stable average execution time of each thread within one read operation. */ \
    M(SettingBool, merge_tree_uniform_read_distribution, true) \
    \
    /** The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization */ \
    M(SettingUInt64, optimize_min_equality_disjunction_chain_length, 3) \
    \
    /** The minimum number of bytes for input/output operations is bypassing the page cache. 0 - disabled. */ \
    M(SettingUInt64, min_bytes_to_use_direct_io, 0) \
    \
    /** Throw an exception if there is an index, and it is not used. */ \
    M(SettingBool, force_index_by_date, 0) \
    M(SettingBool, force_primary_key, 0) \
    \
    /** In the INSERT query with specified columns, fill in the default values ​​only for columns with explicit DEFAULTs. */ \
    M(SettingBool, strict_insert_defaults, 0) \
    \
    /** If the maximum size of mark_cache is exceeded, delete only records older than mark_cache_min_lifetime seconds. */ \
    M(SettingUInt64, mark_cache_min_lifetime, 10000) \
    \
    /** Allows you to use more sources than the number of threads - to more evenly distribute work across threads. \
      * It is assumed that this is a temporary solution, since it will be possible in the future to make the number of sources equal to the number of threads, \
      *  but for each source to dynamically select available work for itself. \
      */ \
    M(SettingFloat, max_streams_to_max_threads_ratio, 1) \
    \
    /** Allows you to select the method of data compression when writing */ \
    M(SettingCompressionMethod, network_compression_method, CompressionMethod::LZ4) \
    \
    /** Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities. */ \
    M(SettingUInt64, priority, 0) \
    \
    /** Log requests and write the log to the system table. */ \
    M(SettingBool, log_queries, 0) \
    \
    /** If query length is greater than specified threshold (in bytes), then cut query when writing to query log. \
      * Also limit length of printed query in ordinary text log. \
      */ \
    M(SettingUInt64, log_queries_cut_to_length, 100000) \
    \
    /** How are distributed subqueries performed inside IN or JOIN sections? */ \
    M(SettingDistributedProductMode, distributed_product_mode, DistributedProductMode::DENY) \
    \
    /** The scheme for executing GLOBAL subqueries. */ \
    M(SettingGlobalSubqueriesMethod, global_subqueries_method, GlobalSubqueriesMethod::PUSH) \
    \
    /** The maximum number of concurrent requests per user. */ \
    M(SettingUInt64, max_concurrent_queries_for_user, 0) \
    \
    /** For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled. */ \
    M(SettingUInt64, insert_quorum, 0) \
    M(SettingMilliseconds, insert_quorum_timeout, 600000) \
    /** For SELECT queries from the replicated table, throw an exception if the replica does not have a chunk written with the quorum; \
      * do not read the parts that have not yet been written with the quorum. */ \
    M(SettingUInt64, select_sequential_consistency, 0) \
    /** The maximum number of different shards and the maximum number of replicas of one shard in the `remote` function. */ \
    M(SettingUInt64, table_function_remote_max_addresses, 1000) \
    /** Maximum number of threads for distributed processing of one query */ \
    M(SettingUInt64, max_distributed_processing_threads, 8) \
    \
    /** Settings to reduce the number of threads in case of slow reads. */ \
    /** Pay attention only to readings that took at least that much time. */ \
    M(SettingMilliseconds,     read_backoff_min_latency_ms, 1000) \
    /** Count events when the bandwidth is less than that many bytes per second. */ \
    M(SettingUInt64,         read_backoff_max_throughput, 1048576) \
    /** Do not pay attention to the event, if the previous one has passed less than a certain amount of time. */ \
    M(SettingMilliseconds,     read_backoff_min_interval_between_events_ms, 1000) \
    /** The number of events after which the number of threads will be reduced. */ \
    M(SettingUInt64,         read_backoff_min_events, 2) \
    \
    /** For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability. */ \
    M(SettingFloat, memory_tracker_fault_probability, 0.) \
    \
    /** Compress the result if the client over HTTP said that it understands data compressed by gzip or deflate */ \
    M(SettingBool, enable_http_compression, 0) \
    /** Compression level - used if the client on HTTP said that it understands data compressed by gzip or deflate */ \
    M(SettingInt64, http_zlib_compression_level, 3) \
    \
    /** If you uncompress the POST data from the client compressed by the native format, do not check the checksum */ \
    M(SettingBool, http_native_compression_disable_checksumming_on_decompress, 0) \
    \
    /** Timeout in seconds */ \
    M(SettingUInt64, resharding_barrier_timeout, 300) \
    \
    /** What aggregate function to use for implementation of count(DISTINCT ...) */ \
    M(SettingString, count_distinct_implementation, "uniqExact") \
    \
    /** Write statistics about read rows, bytes, time elapsed in suitable output formats */ \
    M(SettingBool, output_format_write_statistics, true) \
    \
    /** Write add http CORS header */ \
    M(SettingBool, add_http_cors_header, false) \
    \
    /** Skip columns with unknown names from input data (it works for JSONEachRow and TSKV formats). */ \
    M(SettingBool, input_format_skip_unknown_fields, false) \
    \
    /** For Values format: if field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression. */ \
    M(SettingBool, input_format_values_interpret_expressions, true) \
    \
    /** Controls quoting of 64-bit integers in JSON output format. */ \
    M(SettingBool, output_format_json_quote_64bit_integers, true) \
    \
    /** Rows limit for Pretty formats. */ \
    M(SettingUInt64, output_format_pretty_max_rows, 10000) \
    \
    /** Use client timezone for interpreting DateTime string values, instead of adopting server timezone. */ \
    M(SettingBool, use_client_time_zone, false) \
    \
    /** Send progress notifications using X-ClickHouse-Progress headers. \
      * Some clients do not support high amount of HTTP headers (Python requests in particular), so it is disabled by default. \
      */ \
    M(SettingBool, send_progress_in_http_headers, false) \
    \
    /** Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval. */ \
    M(SettingUInt64, http_headers_progress_interval_ms, 100) \
    \
    /** Do fsync after changing metadata for tables and databases (.sql files). \
      * Could be disabled in case of poor latency on server with high load of DDL queries and high load of disk subsystem. \
      */ \
    M(SettingBool, fsync_metadata, 1) \
    \
    /** Maximum amount of errors while reading text formats (like CSV, TSV). \
      * In case of error, if both values are non-zero, \
      *  and at least absolute or relative amount of errors is lower than corresponding value, \
      *  will skip until next line and continue. \
      */ \
    M(SettingUInt64, input_format_allow_errors_num, 0) \
    M(SettingFloat, input_format_allow_errors_ratio, 0) \
    \
   /** Use NULLs for non-joined rows of outer JOINs. \
     * If false, use default value of corresponding columns data type. \
     */ \
    M(SettingBool, join_use_nulls, 0) \
    /* */ \
    M(SettingUInt64, preferred_block_size_bytes, 1000000) \
   /** If set, distributed queries of Replicated tables will choose servers \
     * with replication delay in seconds less than the specified value (not inclusive). \
     * Zero means do not take delay into account. \
     */ \
    \
    M(SettingUInt64, max_replica_delay_for_distributed_queries, 0) \
   /** Suppose max_replica_delay_for_distributed_queries is set and all replicas for the queried table are stale. \
     * If this setting is enabled, the query will be performed anyway, otherwise the error will be reported. \
     */ \
    M(SettingBool, fallback_to_stale_replicas_for_distributed_queries, 1)


    /// Possible limits for query execution.
    Limits limits;

#define DECLARE(TYPE, NAME, DEFAULT) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_SETTINGS(DECLARE)

#undef DECLARE

    /// Set setting by name.
    void set(const String & name, const Field & value);

    /// Set setting by name. Read value, serialized in binary form from buffer (for inter-server communication).
    void set(const String & name, ReadBuffer & buf);

    /// Skip value, serialized in binary form in buffer.
    void ignore(const String & name, ReadBuffer & buf);

    /// Set setting by name. Read value in text form from string (for example, from configuration file or from URL parameter).
    void set(const String & name, const String & value);

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
      * The profile can also be set using the `set` functions, like the profile setting.
      */
    void setProfile(const String & profile_name, Poco::Util::AbstractConfiguration & config);

    /// Load settings from configuration file, at "path" prefix in configuration.
    void loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

    /// Read settings from buffer. They are serialized as list of contiguous name-value pairs, finished with empty name.
    /// If readonly=1 is set, ignore read settings.
    void deserialize(ReadBuffer & buf);

    /// Write changed settings to buffer. (For example, to be sent to remote server.)
    void serialize(WriteBuffer & buf) const;
};


}
