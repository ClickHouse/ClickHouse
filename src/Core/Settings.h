#pragma once

#include <Common/NamePrompter.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/Defines.h>
#include <IO/ReadSettings.h>
#include <base/unit.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class IColumn;

/** List of settings: type, name, default value, description, flags
  *
  * This looks rather inconvenient. It is done that way to avoid repeating settings in different places.
  * Note: as an alternative, we could implement settings to be completely dynamic in form of map: String -> Field,
  *  but we are not going to do it, because settings is used everywhere as static struct fields.
  *
  * `flags` can be either 0 or IMPORTANT.
  * A setting is "IMPORTANT" if it affects the results of queries and can't be ignored by older versions.
  *
  * When adding new settings that control some backward incompatible changes or when changing some settings values,
  * consider adding them to settings changes history in SettingsChangesHistory.h for special `compatibility` setting
  * to work correctly.
  */

#define COMMON_SETTINGS(M, ALIAS) \
    M(Dialect, dialect, Dialect::clickhouse, "Which dialect will be used to parse query", 0)\
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
    M(UInt64, max_insert_delayed_streams_for_parallel_write, 0, "The maximum number of streams (columns) to delay final part flush. Default - auto (1000 in case of underlying storage supports parallel write, for example S3 and disabled otherwise)", 0) \
    M(MaxThreads, max_final_threads, 0, "The maximum number of threads to read from table with FINAL.", 0) \
    M(UInt64, max_threads_for_indexes, 0, "The maximum number of threads process indices.", 0) \
    M(MaxThreads, max_threads, 0, "The maximum number of threads to execute the request. By default, it is determined automatically.", 0) \
    M(Bool, use_concurrency_control, true, "Respect the server's concurrency control (see the `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores` global server settings). If disabled, it allows using a larger number of threads even if the server is overloaded (not recommended for normal usage, and needed mostly for tests).", 0) \
    M(MaxThreads, max_download_threads, 4, "The maximum number of threads to download data (e.g. for URL engine).", 0) \
    M(UInt64, max_download_buffer_size, 10*1024*1024, "The maximal size of buffer for parallel downloading (e.g. for URL engine) per each thread.", 0) \
    M(UInt64, max_read_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, "The maximum size of the buffer to read from the filesystem.", 0) \
    M(UInt64, max_read_buffer_size_local_fs, 128*1024, "The maximum size of the buffer to read from local filesystem. If set to 0 then max_read_buffer_size will be used.", 0) \
    M(UInt64, max_read_buffer_size_remote_fs, 0, "The maximum size of the buffer to read from remote filesystem. If set to 0 then max_read_buffer_size will be used.", 0) \
    M(UInt64, max_distributed_connections, 1024, "The maximum number of connections for distributed processing of one query (should be greater than max_threads).", 0) \
    M(UInt64, max_query_size, DBMS_DEFAULT_MAX_QUERY_SIZE, "The maximum number of bytes of a query string parsed by the SQL parser. Data in the VALUES clause of INSERT queries is processed by a separate stream parser (that consumes O(1) RAM) and not affected by this restriction.", 0) \
    M(UInt64, interactive_delay, 100000, "The interval in microseconds to check if the request is cancelled, and to send progress info.", 0) \
    M(Seconds, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connection timeout if there are no replicas.", 0) \
    M(Milliseconds, handshake_timeout_ms, 10000, "Timeout for receiving HELLO packet from replicas.", 0) \
    M(Milliseconds, connect_timeout_with_failover_ms, 1000, "Connection timeout for selecting first healthy replica.", 0) \
    M(Milliseconds, connect_timeout_with_failover_secure_ms, 1000, "Connection timeout for selecting first healthy replica (for secure connections).", 0) \
    M(Seconds, receive_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "Timeout for receiving data from network, in seconds. If no bytes were received in this interval, exception is thrown. If you set this setting on client, the 'send_timeout' for the socket will be also set on the corresponding connection end on the server.", 0) \
    M(Seconds, send_timeout, DBMS_DEFAULT_SEND_TIMEOUT_SEC, "Timeout for sending data to network, in seconds. If client needs to sent some data, but it did not able to send any bytes in this interval, exception is thrown. If you set this setting on client, the 'receive_timeout' for the socket will be also set on the corresponding connection end on the server.", 0) \
    M(Seconds, tcp_keep_alive_timeout, 290 /* less than DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC */, "The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes", 0) \
    M(Milliseconds, hedged_connection_timeout_ms, 50, "Connection timeout for establishing connection with replica for Hedged requests", 0) \
    M(Milliseconds, receive_data_timeout_ms, 2000, "Connection timeout for receiving first packet of data or packet with positive progress from replica", 0) \
    M(Bool, use_hedged_requests, true, "Use hedged requests for distributed queries", 0) \
    M(Bool, allow_changing_replica_until_first_data_packet, false, "Allow HedgedConnections to change replica until receiving first data packet", 0) \
    M(Milliseconds, queue_max_wait_ms, 0, "The wait time in the request queue, if the number of concurrent requests exceeds the maximum.", 0) \
    M(Milliseconds, connection_pool_max_wait_ms, 0, "The wait time when the connection pool is full.", 0) \
    M(Milliseconds, replace_running_query_max_wait_ms, 5000, "The wait time for running query with the same query_id to finish when setting 'replace_running_query' is active.", 0) \
    M(Milliseconds, kafka_max_wait_ms, 5000, "The wait time for reading from Kafka before retry.", 0) \
    M(Milliseconds, rabbitmq_max_wait_ms, 5000, "The wait time for reading from RabbitMQ before retry.", 0) \
    M(UInt64, poll_interval, DBMS_DEFAULT_POLL_INTERVAL, "Block at the query wait loop on the server for the specified number of seconds.", 0) \
    M(UInt64, idle_connection_timeout, 3600, "Close idle TCP connections after specified number of seconds.", 0) \
    M(UInt64, distributed_connections_pool_size, 1024, "Maximum number of connections with one remote server in the pool.", 0) \
    M(UInt64, connections_with_failover_max_tries, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES, "The maximum number of attempts to connect to replicas.", 0) \
    M(UInt64, s3_strict_upload_part_size, 0, "The exact size of part to upload during multipart upload to S3 (some implementations does not supports variable size parts).", 0) \
    M(UInt64, s3_min_upload_part_size, 16*1024*1024, "The minimum size of part to upload during multipart upload to S3.", 0) \
    M(UInt64, s3_max_upload_part_size, 5ull*1024*1024*1024, "The maximum size of part to upload during multipart upload to S3.", 0) \
    M(UInt64, s3_upload_part_size_multiply_factor, 2, "Multiply s3_min_upload_part_size by this factor each time s3_multiply_parts_count_threshold parts were uploaded from a single write to S3.", 0) \
    M(UInt64, s3_upload_part_size_multiply_parts_count_threshold, 500, "Each time this number of parts was uploaded to S3, s3_min_upload_part_size is multiplied by s3_upload_part_size_multiply_factor.", 0) \
    M(UInt64, s3_max_inflight_parts_for_one_file, 20, "The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited. You ", 0) \
    M(UInt64, s3_max_single_part_upload_size, 32*1024*1024, "The maximum size of object to upload using singlepart upload to S3.", 0) \
    M(UInt64, azure_max_single_part_upload_size, 100*1024*1024, "The maximum size of object to upload using singlepart upload to Azure blob storage.", 0) \
    M(UInt64, s3_max_single_read_retries, 4, "The maximum number of retries during single S3 read.", 0) \
    M(UInt64, azure_max_single_read_retries, 4, "The maximum number of retries during single Azure blob storage read.", 0) \
    M(UInt64, s3_max_unexpected_write_error_retries, 4, "The maximum number of retries in case of unexpected errors during S3 write.", 0) \
    M(UInt64, s3_max_redirects, 10, "Max number of S3 redirects hops allowed.", 0) \
    M(UInt64, s3_max_connections, 1024, "The maximum number of connections per server.", 0) \
    M(UInt64, s3_max_get_rps, 0, "Limit on S3 GET request per second rate before throttling. Zero means unlimited.", 0) \
    M(UInt64, s3_max_get_burst, 0, "Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_get_rps`", 0) \
    M(UInt64, s3_max_put_rps, 0, "Limit on S3 PUT request per second rate before throttling. Zero means unlimited.", 0) \
    M(UInt64, s3_max_put_burst, 0, "Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_put_rps`", 0) \
    M(UInt64, s3_list_object_keys_size, 1000, "Maximum number of files that could be returned in batch by ListObject request", 0) \
    M(UInt64, azure_list_object_keys_size, 1000, "Maximum number of files that could be returned in batch by ListObject request", 0) \
    M(Bool, s3_truncate_on_insert, false, "Enables or disables truncate before insert in s3 engine tables.", 0) \
    M(Bool, azure_truncate_on_insert, false, "Enables or disables truncate before insert in azure engine tables.", 0) \
    M(Bool, s3_create_new_file_on_insert, false, "Enables or disables creating a new file on each insert in s3 engine tables", 0) \
    M(Bool, s3_skip_empty_files, false, "Allow to skip empty files in s3 table engine", 0) \
    M(Bool, azure_create_new_file_on_insert, false, "Enables or disables creating a new file on each insert in azure engine tables", 0) \
    M(Bool, s3_check_objects_after_upload, false, "Check each uploaded object to s3 with head request to be sure that upload was successful", 0) \
    M(Bool, s3_allow_parallel_part_upload, true, "Use multiple threads for s3 multipart upload. It may lead to slightly higher memory usage", 0) \
    M(Bool, s3_throw_on_zero_files_match, false, "Throw an error, when ListObjects request cannot match any files", 0) \
    M(UInt64, s3_retry_attempts, 10, "Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries", 0) \
    M(UInt64, s3_request_timeout_ms, 3000, "Idleness timeout for sending and receiving data to/from S3. Fail if a single TCP read or write call blocks for this long.", 0) \
    M(Bool, enable_s3_requests_logging, false, "Enable very explicit logging of S3 requests. Makes sense for debug only.", 0) \
    M(String, s3queue_default_zookeeper_path, "/s3queue/", "Default zookeeper path prefix for S3Queue engine", 0) \
    M(UInt64, hdfs_replication, 0, "The actual number of replications can be specified when the hdfs file is created.", 0) \
    M(Bool, hdfs_truncate_on_insert, false, "Enables or disables truncate before insert in s3 engine tables", 0) \
    M(Bool, hdfs_create_new_file_on_insert, false, "Enables or disables creating a new file on each insert in hdfs engine tables", 0) \
    M(Bool, hdfs_skip_empty_files, false, "Allow to skip empty files in hdfs table engine", 0) \
    M(UInt64, hsts_max_age, 0, "Expired time for hsts. 0 means disable HSTS.", 0) \
    M(Bool, extremes, false, "Calculate minimums and maximums of the result columns. They can be output in JSON-formats.", IMPORTANT) \
    M(Bool, use_uncompressed_cache, false, "Whether to use the cache of uncompressed blocks.", 0) \
    M(Bool, replace_running_query, false, "Whether the running request should be canceled with the same id as the new one.", 0) \
    M(UInt64, max_remote_read_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for read.", 0) \
    M(UInt64, max_remote_write_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for write.", 0) \
    M(UInt64, max_local_read_bandwidth, 0, "The maximum speed of local reads in bytes per second.", 0) \
    M(UInt64, max_local_write_bandwidth, 0, "The maximum speed of local writes in bytes per second.", 0) \
    M(Bool, stream_like_engine_allow_direct_select, false, "Allow direct SELECT query for Kafka, RabbitMQ, FileLog, Redis Streams and NATS engines. In case there are attached materialized views, SELECT query is not allowed even if this setting is enabled.", 0) \
    M(String, stream_like_engine_insert_queue, "", "When stream like engine reads from multiple queues, user will need to select one queue to insert into when writing. Used by Redis Streams and NATS.", 0) \
    \
    M(Milliseconds, distributed_directory_monitor_sleep_time_ms, 100, "Sleep time for StorageDistributed DirectoryMonitors, in case of any errors delay grows exponentially.", 0) \
    M(Milliseconds, distributed_directory_monitor_max_sleep_time_ms, 30000, "Maximum sleep time for StorageDistributed DirectoryMonitors, it limits exponential growth too.", 0) \
    \
    M(Bool, distributed_directory_monitor_batch_inserts, false, "Should StorageDistributed DirectoryMonitors try to batch individual inserts into bigger ones.", 0) \
    M(Bool, distributed_directory_monitor_split_batch_on_failure, false, "Should StorageDistributed DirectoryMonitors try to split batch into smaller in case of failures.", 0) \
    \
    M(Bool, optimize_move_to_prewhere, true, "Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree.", 0) \
    M(Bool, optimize_move_to_prewhere_if_final, false, "If query has `FINAL`, the optimization `move_to_prewhere` is not always correct and it is enabled only if both settings `optimize_move_to_prewhere` and `optimize_move_to_prewhere_if_final` are turned on", 0) \
    M(Bool, move_all_conditions_to_prewhere, true, "Move all viable conditions from WHERE to PREWHERE", 0) \
    M(Bool, enable_multiple_prewhere_read_steps, true, "Move more conditions from WHERE to PREWHERE and do reads from disk and filtering in multiple steps if there are multiple conditions combined with AND", 0) \
    M(Bool, move_primary_key_columns_to_end_of_prewhere, true, "Move PREWHERE conditions containing primary key columns to the end of AND chain. It is likely that these conditions are taken into account during primary key analysis and thus will not contribute a lot to PREWHERE filtering.", 0) \
    \
    M(UInt64, alter_sync, 1, "Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.", 0) ALIAS(replication_alter_partitions_sync) \
    M(Int64, replication_wait_for_inactive_replica_timeout, 120, "Wait for inactive replica to execute ALTER/OPTIMIZE. Time in seconds, 0 - do not wait, negative - wait for unlimited time.", 0) \
    \
    M(LoadBalancing, load_balancing, LoadBalancing::RANDOM, "Which replicas (among healthy replicas) to preferably send a query to (on the first attempt) for distributed processing.", 0) \
    M(UInt64, load_balancing_first_offset, 0, "Which replica to preferably send a query when FIRST_OR_RANDOM load balancing strategy is used.", 0) \
    \
    M(TotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE, "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.", IMPORTANT) \
    M(Float, totals_auto_threshold, 0.5, "The threshold for totals_mode = 'auto'.", 0) \
    \
    M(Bool, allow_suspicious_low_cardinality_types, false, "In CREATE TABLE statement allows specifying LowCardinality modifier for types of small fixed size (8 or less). Enabling this may increase merge times and memory consumption.", 0) \
    M(Bool, allow_suspicious_fixed_string_types, false, "In CREATE TABLE statement allows creating columns of type FixedString(n) with n > 256. FixedString with length >= 256 is suspicious and most likely indicates misusage", 0) \
    M(Bool, allow_suspicious_indices, false, "Reject primary/secondary indexes and sorting keys with identical expressions", 0) \
    M(Bool, compile_expressions, false, "Compile some scalar functions and operators to native code.", 0) \
    M(UInt64, min_count_to_compile_expression, 3, "The number of identical expressions before they are JIT-compiled", 0) \
    M(Bool, compile_aggregate_expressions, true, "Compile aggregate functions to native code. This feature has a bug and should not be used.", 0) \
    M(UInt64, min_count_to_compile_aggregate_expression, 3, "The number of identical aggregate expressions before they are JIT-compiled", 0) \
    M(Bool, compile_sort_description, true, "Compile sort description to native code.", 0) \
    M(UInt64, min_count_to_compile_sort_description, 3, "The number of identical sort descriptions before they are JIT-compiled", 0) \
    M(UInt64, group_by_two_level_threshold, 100000, "From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.", 0) \
    M(UInt64, group_by_two_level_threshold_bytes, 50000000, "From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.", 0) \
    M(Bool, distributed_aggregation_memory_efficient, true, "Is the memory-saving mode of distributed aggregation enabled.", 0) \
    M(UInt64, aggregation_memory_efficient_merge_threads, 0, "Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as 'max_threads'.", 0) \
    M(Bool, enable_memory_bound_merging_of_aggregation_results, true, "Enable memory bound merging strategy for aggregation.", 0) \
    M(Bool, enable_positional_arguments, true, "Enable positional arguments in ORDER BY, GROUP BY and LIMIT BY", 0) \
    M(Bool, enable_extended_results_for_datetime_functions, false, "Enable date functions like toLastDayOfMonth return Date32 results (instead of Date results) for Date32/DateTime64 arguments.", 0) \
    M(Bool, allow_nonconst_timezone_arguments, false, "Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*()", 0) \
    \
    M(Bool, group_by_use_nulls, false, "Treat columns mentioned in ROLLUP, CUBE or GROUPING SETS as Nullable", 0) \
    \
    M(UInt64, max_parallel_replicas, 1, "The maximum number of replicas of each shard used when the query is executed. For consistency (to get different parts of the same partition), this option only works for the specified sampling key. The lag of the replicas is not controlled.", 0) \
    M(UInt64, parallel_replicas_count, 0, "This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the number of parallel replicas participating in query processing.", 0) \
    M(UInt64, parallel_replica_offset, 0, "This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the index of the replica participating in query processing among parallel replicas.", 0) \
    M(String, parallel_replicas_custom_key, "", "Custom key assigning work to replicas when parallel replicas are used.", 0) \
    M(ParallelReplicasCustomKeyFilterType, parallel_replicas_custom_key_filter_type, ParallelReplicasCustomKeyFilterType::DEFAULT, "Type of filter to use with custom key for parallel replicas. default - use modulo operation on the custom key, range - use range filter on custom key using all possible values for the value type of custom key.", 0) \
    \
    M(String, cluster_for_parallel_replicas, "default", "Cluster for a shard in which current server is located", 0) \
    M(UInt64, allow_experimental_parallel_reading_from_replicas, 0, "Use all the replicas from a shard for SELECT query execution. Reading is parallelized and coordinated dynamically. 0 - disabled, 1 - enabled, silently disable them in case of failure, 2 - enabled, throw an exception in case of failure", 0) \
    M(Float, parallel_replicas_single_task_marks_count_multiplier, 2, "A multiplier which will be added during calculation for minimal number of marks to retrieve from coordinator. This will be applied only for remote replicas.", 0) \
    M(Bool, parallel_replicas_for_non_replicated_merge_tree, false, "If true, ClickHouse will use parallel replicas algorithm also for non-replicated MergeTree tables", 0) \
    M(UInt64, parallel_replicas_min_number_of_granules_to_enable, 0, "If the number of marks to read is less than the value of this setting - parallel replicas will be disabled", 0) \
    \
    M(Bool, skip_unavailable_shards, false, "If true, ClickHouse silently skips unavailable shards and nodes unresolvable through DNS. Shard is marked as unavailable when none of the replicas can be reached.", 0) \
    \
    M(UInt64, parallel_distributed_insert_select, 0, "Process distributed INSERT SELECT query in the same cluster on local tables on every shard; if set to 1 - SELECT is executed on each shard; if set to 2 - SELECT and INSERT are executed on each shard", 0) \
    M(UInt64, distributed_group_by_no_merge, 0, "If 1, Do not merge aggregation states from different servers for distributed queries (shards will process query up to the Complete stage, initiator just proxies the data from the shards). If 2 the initiator will apply ORDER BY and LIMIT stages (it is not in case when shard process query up to the Complete stage)", 0) \
    M(UInt64, distributed_push_down_limit, 1, "If 1, LIMIT will be applied on each shard separately. Usually you don't need to use it, since this will be done automatically if it is possible, i.e. for simple query SELECT FROM LIMIT.", 0) \
    M(Bool, optimize_distributed_group_by_sharding_key, true, "Optimize GROUP BY sharding_key queries (by avoiding costly aggregation on the initiator server).", 0) \
    M(UInt64, optimize_skip_unused_shards_limit, 1000, "Limit for number of sharding key values, turns off optimize_skip_unused_shards if the limit is reached", 0) \
    M(Bool, optimize_skip_unused_shards, false, "Assumes that data is distributed by sharding_key. Optimization to skip unused shards if SELECT query filters by sharding_key.", 0) \
    M(Bool, optimize_skip_unused_shards_rewrite_in, true, "Rewrite IN in query for remote shards to exclude values that does not belong to the shard (requires optimize_skip_unused_shards)", 0) \
    M(Bool, allow_nondeterministic_optimize_skip_unused_shards, false, "Allow non-deterministic functions (includes dictGet) in sharding_key for optimize_skip_unused_shards", 0) \
    M(UInt64, force_optimize_skip_unused_shards, 0, "Throw an exception if unused shards cannot be skipped (1 - throw only if the table has the sharding key, 2 - always throw.", 0) \
    M(UInt64, optimize_skip_unused_shards_nesting, 0, "Same as optimize_skip_unused_shards, but accept nesting level until which it will work.", 0) \
    M(UInt64, force_optimize_skip_unused_shards_nesting, 0, "Same as force_optimize_skip_unused_shards, but accept nesting level until which it will work.", 0) \
    \
    M(Bool, input_format_parallel_parsing, true, "Enable parallel parsing for some data formats.", 0) \
    M(UInt64, min_chunk_bytes_for_parallel_parsing, (10 * 1024 * 1024), "The minimum chunk size in bytes, which each thread will parse in parallel.", 0) \
    M(Bool, output_format_parallel_formatting, true, "Enable parallel formatting for some data formats.", 0) \
    \
    M(UInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192), "If at least as many lines are read from one file, the reading can be parallelized.", 0) \
    M(UInt64, merge_tree_min_bytes_for_concurrent_read, (24 * 10 * 1024 * 1024), "If at least as many bytes are read from one file, the reading can be parallelized.", 0) \
    M(UInt64, merge_tree_min_rows_for_seek, 0, "You can skip reading more than that number of rows at the price of one seek per file.", 0) \
    M(UInt64, merge_tree_min_bytes_for_seek, 0, "You can skip reading more than that number of bytes at the price of one seek per file.", 0) \
    M(UInt64, merge_tree_coarse_index_granularity, 8, "If the index segment can contain the required keys, divide it into as many parts and recursively check them.", 0) \
    M(UInt64, merge_tree_max_rows_to_use_cache, (128 * 8192), "The maximum number of rows per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)", 0) \
    M(UInt64, merge_tree_max_bytes_to_use_cache, (192 * 10 * 1024 * 1024), "The maximum number of bytes per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)", 0) \
    M(Bool, do_not_merge_across_partitions_select_final, false, "Merge parts only in one partition in select final", 0) \
    M(Bool, allow_experimental_inverted_index, false, "If it is set to true, allow to use experimental inverted index.", 0) \
    \
    M(UInt64, mysql_max_rows_to_insert, 65536, "The maximum number of rows in MySQL batch insertion of the MySQL storage engine", 0) \
    M(Bool, use_mysql_types_in_show_columns, false, "Use MySQL converted types when connected via MySQL compatibility for show columns query", 0) \
    \
    M(UInt64, optimize_min_equality_disjunction_chain_length, 3, "The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization ", 0) \
    \
    M(UInt64, min_bytes_to_use_direct_io, 0, "The minimum number of bytes for reading the data with O_DIRECT option during SELECT queries execution. 0 - disabled.", 0) \
    M(UInt64, min_bytes_to_use_mmap_io, 0, "The minimum number of bytes for reading the data with mmap option during SELECT queries execution. 0 - disabled.", 0) \
    M(Bool, checksum_on_read, true, "Validate checksums on reading. It is enabled by default and should be always enabled in production. Please do not expect any benefits in disabling this setting. It may only be used for experiments and benchmarks. The setting only applicable for tables of MergeTree family. Checksums are always validated for other table engines and when receiving data over network.", 0) \
    \
    M(Bool, force_index_by_date, false, "Throw an exception if there is a partition key in a table, and it is not used.", 0) \
    M(Bool, force_primary_key, false, "Throw an exception if there is primary key in a table, and it is not used.", 0) \
    M(Bool, use_skip_indexes, true, "Use data skipping indexes during query execution.", 0) \
    M(Bool, use_skip_indexes_if_final, false, "If query has FINAL, then skipping data based on indexes may produce incorrect result, hence disabled by default.", 0) \
    M(String, ignore_data_skipping_indices, "", "Comma separated list of strings or literals with the name of the data skipping indices that should be excluded during query execution.", 0) \
    \
    M(String, force_data_skipping_indices, "", "Comma separated list of strings or literals with the name of the data skipping indices that should be used during query execution, otherwise an exception will be thrown.", 0) \
    \
    M(Float, max_streams_to_max_threads_ratio, 1, "Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution, since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.", 0) \
    M(Float, max_streams_multiplier_for_merge_tables, 5, "Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and especially helpful when merged tables differ in size.", 0) \
    \
    M(String, network_compression_method, "LZ4", "Allows you to select the method of data compression when writing.", 0) \
    \
    M(Int64, network_zstd_compression_level, 1, "Allows you to select the level of ZSTD compression.", 0) \
    \
    M(Int64, zstd_window_log_max, 0, "Allows you to select the max window log of ZSTD (it will not be used for MergeTree family)", 0) \
    \
    M(UInt64, priority, 0, "Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.", 0) \
    M(Int64, os_thread_priority, 0, "If non zero - set corresponding 'nice' value for query processing threads. Can be used to adjust query priority for OS scheduler.", 0) \
    \
    M(Bool, log_queries, true, "Log requests and write the log to the system table.", 0) \
    M(Bool, log_formatted_queries, false, "Log formatted queries and write the log to the system table.", 0) \
    M(LogQueriesType, log_queries_min_type, QueryLogElementType::QUERY_START, "Minimal type in query_log to log, possible values (from low to high): QUERY_START, QUERY_FINISH, EXCEPTION_BEFORE_START, EXCEPTION_WHILE_PROCESSING.", 0) \
    M(Milliseconds, log_queries_min_query_duration_ms, 0, "Minimal time for the query to run, to get to the query_log/query_thread_log/query_views_log.", 0) \
    M(UInt64, log_queries_cut_to_length, 100000, "If query length is greater than specified threshold (in bytes), then cut query when writing to query log. Also limit length of printed query in ordinary text log.", 0) \
    M(Float, log_queries_probability, 1., "Log queries with the specified probabality.", 0) \
    \
    M(Bool, log_processors_profiles, false, "Log Processors profile events.", 0) \
    M(DistributedProductMode, distributed_product_mode, DistributedProductMode::DENY, "How are distributed subqueries performed inside IN or JOIN sections?", IMPORTANT) \
    \
    M(UInt64, max_concurrent_queries_for_all_users, 0, "The maximum number of concurrent requests for all users.", 0) \
    M(UInt64, max_concurrent_queries_for_user, 0, "The maximum number of concurrent requests per user.", 0) \
    \
    M(Bool, insert_deduplicate, true, "For INSERT queries in the replicated table, specifies that deduplication of insertings blocks should be performed", 0) \
    M(Bool, async_insert_deduplicate, false, "For async INSERT queries in the replicated table, specifies that deduplication of insertings blocks should be performed", 0) \
    \
    M(UInt64Auto, insert_quorum, 0, "For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled, 'auto' - use majority", 0) \
    M(Milliseconds, insert_quorum_timeout, 600000, "If the quorum of replicas did not meet in specified time (in milliseconds), exception will be thrown and insertion is aborted.", 0) \
    M(Bool, insert_quorum_parallel, true, "For quorum INSERT queries - enable to make parallel inserts without linearizability", 0) \
    M(UInt64, select_sequential_consistency, 0, "For SELECT queries from the replicated table, throw an exception if the replica does not have a chunk written with the quorum; do not read the parts that have not yet been written with the quorum.", 0) \
    M(UInt64, table_function_remote_max_addresses, 1000, "The maximum number of different shards and the maximum number of replicas of one shard in the `remote` function.", 0) \
    M(Milliseconds, read_backoff_min_latency_ms, 1000, "Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.", 0) \
    M(UInt64, read_backoff_max_throughput, 1048576, "Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.", 0) \
    M(Milliseconds, read_backoff_min_interval_between_events_ms, 1000, "Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.", 0) \
    M(UInt64, read_backoff_min_events, 2, "Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.", 0) \
    \
    M(UInt64, read_backoff_min_concurrency, 1, "Settings to try keeping the minimal number of threads in case of slow reads.", 0) \
    \
    M(Float, memory_tracker_fault_probability, 0., "For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.", 0) \
    \
    M(Bool, enable_http_compression, false, "Compress the result if the client over HTTP said that it understands data compressed by gzip, deflate, zstd, br, lz4, bz2, xz.", 0) \
    M(Int64, http_zlib_compression_level, 3, "Compression level - used if the client on HTTP said that it understands data compressed by gzip or deflate.", 0) \
    \
    M(Bool, http_native_compression_disable_checksumming_on_decompress, false, "If you uncompress the POST data from the client compressed by the native format, do not check the checksum.", 0) \
    \
    M(String, count_distinct_implementation, "uniqExact", "What aggregate function to use for implementation of count(DISTINCT ...)", 0) \
    \
    M(Bool, add_http_cors_header, false, "Write add http CORS header.", 0) \
    \
    M(UInt64, max_http_get_redirects, 0, "Max number of http GET redirects hops allowed. Ensures additional security measures are in place to prevent a malicious server to redirect your requests to unexpected services.\n\nIt is the case when an external server redirects to another address, but that address appears to be internal to the company's infrastructure, and by sending an HTTP request to an internal server, you could request an internal API from the internal network, bypassing the auth, or even query other services, such as Redis or Memcached. When you don't have an internal infrastructure (including something running on your localhost), or you trust the server, it is safe to allow redirects. Although keep in mind, that if the URL uses HTTP instead of HTTPS, and you will have to trust not only the remote server but also your ISP and every network in the middle.", 0) \
    \
    M(Bool, use_client_time_zone, false, "Use client timezone for interpreting DateTime string values, instead of adopting server timezone.", 0) \
    \
    M(Bool, send_progress_in_http_headers, false, "Send progress notifications using X-ClickHouse-Progress headers. Some clients do not support high amount of HTTP headers (Python requests in particular), so it is disabled by default.", 0) \
    \
    M(UInt64, http_headers_progress_interval_ms, 100, "Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.", 0) \
    M(Bool, http_wait_end_of_query, false, "Enable HTTP response buffering on the server-side.", 0) \
    M(UInt64, http_response_buffer_size, 0, "The number of bytes to buffer in the server memory before sending a HTTP response to the client or flushing to disk (when http_wait_end_of_query is enabled).", 0) \
    \
    M(Bool, fsync_metadata, true, "Do fsync after changing metadata for tables and databases (.sql files). Could be disabled in case of poor latency on server with high load of DDL queries and high load of disk subsystem.", 0) \
    \
    M(Bool, join_use_nulls, false, "Use NULLs for non-joined rows of outer JOINs for types that can be inside Nullable. If false, use default value of corresponding columns data type.", IMPORTANT) \
    \
    M(JoinStrictness, join_default_strictness, JoinStrictness::All, "Set default strictness in JOIN query. Possible values: empty string, 'ANY', 'ALL'. If empty, query without strictness will throw exception.", 0) \
    M(Bool, any_join_distinct_right_table_keys, false, "Enable old ANY JOIN logic with many-to-one left-to-right table keys mapping for all ANY JOINs. It leads to confusing not equal results for 't1 ANY LEFT JOIN t2' and 't2 ANY RIGHT JOIN t1'. ANY RIGHT JOIN needs one-to-many keys mapping to be consistent with LEFT one.", IMPORTANT) \
    M(Bool, single_join_prefer_left_table, true, "For single JOIN in case of identifier ambiguity prefer left table", IMPORTANT) \
    \
    M(UInt64, preferred_block_size_bytes, 1000000, "This setting adjusts the data block size for query processing and represents additional fine tune to the more rough 'max_block_size' setting. If the columns are large and with 'max_block_size' rows the block size is likely to be larger than the specified amount of bytes, its size will be lowered for better CPU cache locality.", 0) \
    \
    M(UInt64, max_replica_delay_for_distributed_queries, 300, "If set, distributed queries of Replicated tables will choose servers with replication delay in seconds less than the specified value (not inclusive). Zero means do not take delay into account.", 0) \
    M(Bool, fallback_to_stale_replicas_for_distributed_queries, true, "Suppose max_replica_delay_for_distributed_queries is set and all replicas for the queried table are stale. If this setting is enabled, the query will be performed anyway, otherwise the error will be reported.", 0) \
    M(UInt64, preferred_max_column_in_block_size_bytes, 0, "Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.", 0) \
    \
    M(UInt64, parts_to_delay_insert, 0, "If the destination table contains at least that many active parts in a single partition, artificially slow down insert into table.", 0) \
    M(UInt64, parts_to_throw_insert, 0, "If more than this number active parts in a single partition of the destination table, throw 'Too many parts ...' exception.", 0) \
    M(UInt64, number_of_mutations_to_delay, 0, "If the mutated table contains at least that many unfinished mutations, artificially slow down mutations of table. 0 - disabled", 0) \
    M(UInt64, number_of_mutations_to_throw, 0, "If the mutated table contains at least that many unfinished mutations, throw 'Too many mutations ...' exception. 0 - disabled", 0) \
    M(Bool, insert_distributed_sync, false, "If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster.", 0) \
    M(UInt64, insert_distributed_timeout, 0, "Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.", 0) \
    M(Int64, distributed_ddl_task_timeout, 180, "Timeout for DDL query responses from all hosts in cluster. If a ddl request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite. Zero means async mode.", 0) \
    M(Milliseconds, stream_flush_interval_ms, 7500, "Timeout for flushing data from streaming storages.", 0) \
    M(Milliseconds, stream_poll_timeout_ms, 500, "Timeout for polling data from/to streaming storages.", 0) \
    \
    M(Bool, final, false, "Query with the FINAL modifier by default. If the engine does not support final, it does not have any effect. On queries with multiple tables final is applied only on those that support it. It also works on distributed tables", 0) \
    \
    M(Bool, partial_result_on_first_cancel, false, "Allows query to return a partial result after cancel.", 0) \
    \
    M(Bool, ignore_on_cluster_for_replicated_udf_queries, false, "Ignore ON CLUSTER clause for replicated UDF management queries.", 0) \
    M(Bool, ignore_on_cluster_for_replicated_access_entities_queries, false, "Ignore ON CLUSTER clause for replicated access entities management queries.", 0) \
    /** Settings for testing hedged requests */ \
    M(Milliseconds, sleep_in_send_tables_status_ms, 0, "Time to sleep in sending tables status response in TCPHandler", 0) \
    M(Milliseconds, sleep_in_send_data_ms, 0, "Time to sleep in sending data in TCPHandler", 0) \
    M(Milliseconds, sleep_after_receiving_query_ms, 0, "Time to sleep after receiving query in TCPHandler", 0) \
    M(UInt64, unknown_packet_in_send_data, 0, "Send unknown packet instead of data Nth data packet", 0) \
    \
    M(Bool, insert_allow_materialized_columns, false, "If setting is enabled, Allow materialized columns in INSERT.", 0) \
    M(Seconds, http_connection_timeout, DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, "HTTP connection timeout.", 0) \
    M(Seconds, http_send_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, "HTTP send timeout", 0) \
    M(Seconds, http_receive_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, "HTTP receive timeout", 0) \
    M(UInt64, http_max_uri_size, 1048576, "Maximum URI length of HTTP request", 0) \
    M(UInt64, http_max_fields, 1000000, "Maximum number of fields in HTTP header", 0) \
    M(UInt64, http_max_field_name_size, 128 * 1024, "Maximum length of field name in HTTP header", 0) \
    M(UInt64, http_max_field_value_size, 128 * 1024, "Maximum length of field value in HTTP header", 0) \
    M(UInt64, http_max_chunk_size, 100_GiB, "Maximum value of a chunk size in HTTP chunked transfer encoding", 0) \
    M(Bool, http_skip_not_found_url_for_globs, true, "Skip url's for globs with HTTP_NOT_FOUND error", 0) \
    M(Bool, optimize_throw_if_noop, false, "If setting is enabled and OPTIMIZE query didn't actually assign a merge then an explanatory exception is thrown", 0) \
    M(Bool, use_index_for_in_with_subqueries, true, "Try using an index if there is a subquery or a table expression on the right side of the IN operator.", 0) \
    M(UInt64, use_index_for_in_with_subqueries_max_values, 0, "The maximum size of set in the right hand side of the IN operator to use table index for filtering. It allows to avoid performance degradation and higher memory usage due to preparation of additional data structures for large queries. Zero means no limit.", 0) \
    M(Bool, joined_subquery_requires_alias, true, "Force joined subqueries and table functions to have aliases for correct name qualification.", 0) \
    M(Bool, empty_result_for_aggregation_by_empty_set, false, "Return empty result when aggregating without keys on empty set.", 0) \
    M(Bool, empty_result_for_aggregation_by_constant_keys_on_empty_set, true, "Return empty result when aggregating by constant keys on empty set.", 0) \
    M(Bool, allow_distributed_ddl, true, "If it is set to true, then a user is allowed to executed distributed DDL queries.", 0) \
    M(Bool, allow_suspicious_codecs, false, "If it is set to true, allow to specify meaningless compression codecs.", 0) \
    M(Bool, allow_experimental_codecs, false, "If it is set to true, allow to specify experimental compression codecs (but we don't have those yet and this option does nothing).", 0) \
    M(Bool, enable_deflate_qpl_codec, false, "Enable/disable the DEFLATE_QPL codec.", 0) \
    M(UInt64, query_profiler_real_time_period_ns, QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS, "Period for real clock timer of query profiler (in nanoseconds). Set 0 value to turn off the real clock query profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", 0) \
    M(UInt64, query_profiler_cpu_time_period_ns, QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS, "Period for CPU clock timer of query profiler (in nanoseconds). Set 0 value to turn off the CPU clock query profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", 0) \
    M(Bool, metrics_perf_events_enabled, false, "If enabled, some of the perf events will be measured throughout queries' execution.", 0) \
    M(String, metrics_perf_events_list, "", "Comma separated list of perf metrics that will be measured throughout queries' execution. Empty means all events. See PerfEventInfo in sources for the available events.", 0) \
    M(Float, opentelemetry_start_trace_probability, 0., "Probability to start an OpenTelemetry trace for an incoming query.", 0) \
    M(Bool, opentelemetry_trace_processors, false, "Collect OpenTelemetry spans for processors.", 0) \
    M(Bool, prefer_column_name_to_alias, false, "Prefer using column names instead of aliases if possible.", 0) \
    M(Bool, allow_experimental_analyzer, false, "Allow experimental analyzer", 0) \
    M(Bool, prefer_global_in_and_join, false, "If enabled, all IN/JOIN operators will be rewritten as GLOBAL IN/JOIN. It's useful when the to-be-joined tables are only available on the initiator and we need to always scatter their data on-the-fly during distributed processing with the GLOBAL keyword. It's also useful to reduce the need to access the external sources joining external tables.", 0) \
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
    M(UInt64, max_rows_to_read_leaf, 0, "Limit on read rows on the leaf nodes for distributed queries. Limit is applied for local reads only excluding the final merge stage on the root node.", 0) \
    M(UInt64, max_bytes_to_read_leaf, 0, "Limit on read bytes (after decompression) on the leaf nodes for distributed queries. Limit is applied for local reads only excluding the final merge stage on the root node.", 0) \
    M(OverflowMode, read_overflow_mode_leaf, OverflowMode::THROW, "What to do when the leaf limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_to_group_by, 0, "If aggregation during GROUP BY is generating more than specified number of rows (unique GROUP BY keys), the behavior will be determined by the 'group_by_overflow_mode' which by default is - throw an exception, but can be also switched to an approximate GROUP BY mode.", 0) \
    M(OverflowModeGroupBy, group_by_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, max_bytes_before_external_group_by, 0, "If memory usage during GROUP BY operation is exceeding this threshold in bytes, activate the 'external aggregation' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    \
    M(UInt64, max_rows_to_sort, 0, "If more than specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    M(UInt64, max_bytes_to_sort, 0, "If more than specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    M(OverflowMode, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, max_bytes_before_external_sort, 0, "If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the 'external sorting' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    M(UInt64, max_bytes_before_remerge_sort, 1000000000, "In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.", 0) \
    M(Float, remerge_sort_lowered_memory_bytes_ratio, 2., "If memory usage after remerge does not reduced by this ratio, remerge will be disabled.", 0) \
    \
    M(UInt64, max_result_rows, 0, "Limit on result size in rows. The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold.", 0) \
    M(UInt64, max_result_bytes, 0, "Limit on result size in bytes (uncompressed).  The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold. Caveats: the result size in memory is taken into account for this threshold. Even if the result size is small, it can reference larger data structures in memory, representing dictionaries of LowCardinality columns, and Arenas of AggregateFunction columns, so the threshold can be exceeded despite the small result size. The setting is fairly low level and should be used with caution.", 0) \
    M(OverflowMode, result_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    /* TODO: Check also when merging and finalizing aggregate functions. */ \
    M(Seconds, max_execution_time, 0, "If query run time exceeded the specified number of seconds, the behavior will be determined by the 'timeout_overflow_mode' which by default is - throw an exception. Note that the timeout is checked and query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting.", 0) \
    M(OverflowMode, timeout_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, min_execution_speed, 0, "Minimum number of execution rows per second.", 0) \
    M(UInt64, max_execution_speed, 0, "Maximum number of execution rows per second.", 0) \
    M(UInt64, min_execution_speed_bytes, 0, "Minimum number of execution bytes per second.", 0) \
    M(UInt64, max_execution_speed_bytes, 0, "Maximum number of execution bytes per second.", 0) \
    M(Seconds, timeout_before_checking_execution_speed, 10, "Check that the speed is not too low after the specified time has elapsed.", 0) \
    \
    M(UInt64, max_columns_to_read, 0, "If a query requires reading more than specified number of columns, exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.", 0) \
    M(UInt64, max_temporary_columns, 0, "If a query generates more than the specified number of temporary columns in memory as a result of intermediate calculation, exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.", 0) \
    M(UInt64, max_temporary_non_const_columns, 0, "Similar to the 'max_temporary_columns' setting but applies only to non-constant columns. This makes sense, because constant columns are cheap and it is reasonable to allow more of them.", 0) \
    \
    M(UInt64, max_sessions_for_user, 0, "Maximum number of simultaneous sessions for a user.", 0) \
    \
    M(UInt64, max_subquery_depth, 100, "If a query has more than specified number of nested subqueries, throw an exception. This allows you to have a sanity check to protect the users of your cluster from going insane with their queries.", 0) \
    M(UInt64, max_analyze_depth, 5000, "Maximum number of analyses performed by interpreter.", 0) \
    M(UInt64, max_ast_depth, 1000, "Maximum depth of query syntax tree. Checked after parsing.", 0) \
    M(UInt64, max_ast_elements, 50000, "Maximum size of query syntax tree in number of nodes. Checked after parsing.", 0) \
    M(UInt64, max_expanded_ast_elements, 500000, "Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.", 0) \
    \
    M(UInt64, readonly, 0, "0 - no read-only restrictions. 1 - only read requests, as well as changing explicitly allowed settings. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.", 0) \
    \
    M(UInt64, max_rows_in_set, 0, "Maximum size of the set (in number of elements) resulting from the execution of the IN section.", 0) \
    M(UInt64, max_bytes_in_set, 0, "Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.", 0) \
    M(OverflowMode, set_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_in_join, 0, "Maximum size of the hash table for JOIN (in number of rows).", 0) \
    M(UInt64, max_bytes_in_join, 0, "Maximum size of the hash table for JOIN (in number of bytes in memory).", 0) \
    M(OverflowMode, join_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(Bool, join_any_take_last_row, false, "When disabled (default) ANY JOIN will take the first found row for a key. When enabled, it will take the last row seen if there are multiple rows for the same key.", IMPORTANT) \
    M(JoinAlgorithm, join_algorithm, JoinAlgorithm::DEFAULT, "Specify join algorithm.", 0) \
    M(UInt64, default_max_bytes_in_join, 1000000000, "Maximum size of right-side table if limit is required but max_bytes_in_join is not set.", 0) \
    M(UInt64, partial_merge_join_left_table_buffer_bytes, 0, "If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread.", 0) \
    M(UInt64, partial_merge_join_rows_in_right_blocks, 65536, "Split right-hand joining data in blocks of specified size. It's a portion of data indexed by min-max values and possibly unloaded on disk.", 0) \
    M(UInt64, join_on_disk_max_files_to_merge, 64, "For MergeJoin on disk set how much files it's allowed to sort simultaneously. Then this value bigger then more memory used and then less disk I/O needed. Minimum is 2.", 0) \
    M(UInt64, max_rows_in_set_to_optimize_join, 100'000, "Maximal size of the set to filter joined tables by each other row sets before joining. 0 - disable.", 0) \
    \
    M(Bool, compatibility_ignore_collation_in_create_table, true, "Compatibility ignore collation in create table", 0) \
    \
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
    M(UInt64, memory_overcommit_ratio_denominator, 1_GiB, "It represents soft memory limit on the user level. This value is used to compute query overcommit ratio.", 0) \
    M(UInt64, max_memory_usage_for_user, 0, "Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.", 0) \
    M(UInt64, memory_overcommit_ratio_denominator_for_user, 1_GiB, "It represents soft memory limit on the global level. This value is used to compute query overcommit ratio.", 0) \
    M(UInt64, max_untracked_memory, (4 * 1024 * 1024), "Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when amount (in absolute value) becomes larger than specified value. If the value is higher than 'memory_profiler_step' it will be effectively lowered to 'memory_profiler_step'.", 0) \
    M(UInt64, memory_profiler_step, (4 * 1024 * 1024), "Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stack trace. Zero means disabled memory profiler. Values lower than a few megabytes will slow down query processing.", 0) \
    M(Float, memory_profiler_sample_probability, 0., "Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability is for every alloc/free regardless to the size of the allocation (can be changed with `memory_profiler_sample_min_allocation_size` and `memory_profiler_sample_max_allocation_size`). Note that sampling happens only when the amount of untracked memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine grained sampling.", 0) \
    M(UInt64, memory_profiler_sample_min_allocation_size, 0, "Collect random allocations of size greater or equal than specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.", 0) \
    M(UInt64, memory_profiler_sample_max_allocation_size, 0, "Collect random allocations of size less or equal than specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.", 0) \
    M(Bool, trace_profile_events, false, "Send to system.trace_log profile event and value of increment on each increment with 'ProfileEvent' trace_type", 0) \
    \
    M(UInt64, memory_usage_overcommit_max_wait_microseconds, 5'000'000, "Maximum time thread will wait for memory to be freed in the case of memory overcommit. If timeout is reached and memory is not freed, exception is thrown.", 0) \
    \
    M(UInt64, max_network_bandwidth, 0, "The maximum speed of data exchange over the network in bytes per second for a query. Zero means unlimited.", 0) \
    M(UInt64, max_network_bytes, 0, "The maximum number of bytes (compressed) to receive or transmit over the network for execution of the query.", 0) \
    M(UInt64, max_network_bandwidth_for_user, 0, "The maximum speed of data exchange over the network in bytes per second for all concurrently running user queries. Zero means unlimited.", 0)\
    M(UInt64, max_network_bandwidth_for_all_users, 0, "The maximum speed of data exchange over the network in bytes per second for all concurrently running queries. Zero means unlimited.", 0) \
    \
    M(UInt64, max_temporary_data_on_disk_size_for_user, 0, "The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries. Zero means unlimited.", 0)\
    M(UInt64, max_temporary_data_on_disk_size_for_query, 0, "The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries. Zero means unlimited.", 0)\
    \
    M(UInt64, backup_restore_keeper_max_retries, 20, "Max retries for keeper operations during backup or restore", 0) \
    M(UInt64, backup_restore_keeper_retry_initial_backoff_ms, 100, "Initial backoff timeout for [Zoo]Keeper operations during backup or restore", 0) \
    M(UInt64, backup_restore_keeper_retry_max_backoff_ms, 5000, "Max backoff timeout for [Zoo]Keeper operations during backup or restore", 0) \
    M(Float,  backup_restore_keeper_fault_injection_probability, 0.0f, "Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f]", 0) \
    M(UInt64, backup_restore_keeper_fault_injection_seed, 0, "0 - random seed, otherwise the setting value", 0) \
    M(UInt64, backup_restore_keeper_value_max_size, 1048576, "Maximum size of data of a [Zoo]Keeper's node during backup", 0) \
    M(UInt64, backup_restore_batch_size_for_keeper_multiread, 10000, "Maximum size of batch for multiread request to [Zoo]Keeper during backup or restore", 0) \
    M(UInt64, max_backup_bandwidth, 0, "The maximum read speed in bytes per second for particular backup on server. Zero means unlimited.", 0) \
    \
    M(Bool, log_profile_events, true, "Log query performance statistics into the query_log, query_thread_log and query_views_log.", 0) \
    M(Bool, log_query_settings, true, "Log query settings into the query_log.", 0) \
    M(Bool, log_query_threads, false, "Log query threads into system.query_thread_log table. This setting have effect only when 'log_queries' is true.", 0) \
    M(Bool, log_query_views, true, "Log query dependent views into system.query_views_log table. This setting have effect only when 'log_queries' is true.", 0) \
    M(String, log_comment, "", "Log comment into system.query_log table and server log. It can be set to arbitrary string no longer than max_query_size.", 0) \
    M(LogsLevel, send_logs_level, LogsLevel::fatal, "Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(String, send_logs_source_regexp, "", "Send server text logs with specified regexp to match log source name. Empty means all sources.", 0) \
    M(Bool, enable_optimize_predicate_expression, true, "If it is set to true, optimize predicates to subqueries.", 0) \
    M(Bool, enable_optimize_predicate_expression_to_final_subquery, true, "Allow push predicate to final subquery.", 0) \
    M(Bool, allow_push_predicate_when_subquery_contains_with, true, "Allows push predicate when subquery contains WITH clause", 0) \
    \
    M(UInt64, low_cardinality_max_dictionary_size, 8192, "Maximum size (in rows) of shared global dictionary for LowCardinality type.", 0) \
    M(Bool, low_cardinality_use_single_dictionary_for_part, false, "LowCardinality type serialization setting. If is true, than will use additional keys when global dictionary overflows. Otherwise, will create several shared dictionaries.", 0) \
    M(Bool, decimal_check_overflow, true, "Check overflow of decimal arithmetic/comparison operations", 0) \
    M(Bool, allow_custom_error_code_in_throwif, false, "Enable custom error code in function throwIf(). If true, thrown exceptions may have unexpected error codes.", 0) \
    \
    M(Bool, prefer_localhost_replica, true, "If it's true then queries will be always sent to local replica (if it exists). If it's false then replica to send a query will be chosen between local and remote ones according to load_balancing", 0) \
    M(UInt64, max_fetch_partition_retries_count, 5, "Amount of retries while fetching partition from another host.", 0) \
    M(UInt64, http_max_multipart_form_data_size, 1024 * 1024 * 1024, "Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in user profile. Note that content is parsed and external tables are created in memory before start of query execution. And this is the only limit that has effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).", 0) \
    M(Bool, calculate_text_stack_trace, true, "Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when huge amount of wrong queries are executed. In normal cases you should not disable this option.", 0) \
    M(Bool, enable_job_stack_trace, false, "Output stack trace of a job creator when job results in exception", 0) \
    M(Bool, allow_ddl, true, "If it is set to true, then a user is allowed to executed DDL queries.", 0) \
    M(Bool, parallel_view_processing, false, "Enables pushing to attached views concurrently instead of sequentially.", 0) \
    M(Bool, enable_unaligned_array_join, false, "Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.", 0) \
    M(Bool, optimize_read_in_order, true, "Enable ORDER BY optimization for reading data in corresponding order in MergeTree tables.", 0) \
    M(Bool, optimize_read_in_window_order, true, "Enable ORDER BY optimization in window clause for reading data in corresponding order in MergeTree tables.", 0) \
    M(Bool, optimize_aggregation_in_order, false, "Enable GROUP BY optimization for aggregating data in corresponding order in MergeTree tables.", 0) \
    M(UInt64, aggregation_in_order_max_block_bytes, 50000000, "Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.", 0) \
    M(UInt64, read_in_order_two_level_merge_threshold, 100, "Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.", 0) \
    M(Bool, low_cardinality_allow_in_native_format, true, "Use LowCardinality type in Native format. Otherwise, convert LowCardinality columns to ordinary for select query, and convert ordinary columns to required LowCardinality for insert query.", 0) \
    M(Bool, cancel_http_readonly_queries_on_client_close, false, "Cancel HTTP readonly queries when a client closes the connection without waiting for response.", 0) \
    M(Bool, external_table_functions_use_nulls, true, "If it is set to true, external table functions will implicitly use Nullable type if needed. Otherwise NULLs will be substituted with default values. Currently supported only by 'mysql', 'postgresql' and 'odbc' table functions.", 0) \
    M(Bool, external_table_strict_query, false, "If it is set to true, transforming expression to local filter is forbidden for queries to external tables.", 0) \
    \
    M(Bool, allow_hyperscan, true, "Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.", 0) \
    M(UInt64, max_hyperscan_regexp_length, 0, "Max length of regexp than can be used in hyperscan multi-match functions. Zero means unlimited.", 0) \
    M(UInt64, max_hyperscan_regexp_total_length, 0, "Max total length of all regexps than can be used in hyperscan multi-match functions (per every function). Zero means unlimited.", 0) \
    M(Bool, reject_expensive_hyperscan_regexps, true, "Reject patterns which will likely be expensive to evaluate with hyperscan (due to NFA state explosion)", 0) \
    M(Bool, allow_simdjson, true, "Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.", 0) \
    M(Bool, allow_introspection_functions, false, "Allow functions for introspection of ELF and DWARF for query profiling. These functions are slow and may impose security considerations.", 0) \
    \
    M(Bool, allow_execute_multiif_columnar, true, "Allow execute multiIf function columnar", 0) \
    M(Bool, formatdatetime_f_prints_single_zero, false, "Formatter '%f' in function 'formatDateTime()' produces a single zero instead of six zeros if the formatted value has no fractional seconds.", 0) \
    M(Bool, formatdatetime_parsedatetime_m_is_month_name, true, "Formatter '%M' in functions 'formatDateTime()' and 'parseDateTime()' produces the month name instead of minutes.", 0) \
    \
    M(UInt64, max_partitions_per_insert_block, 100, "Limit maximum number of partitions in single INSERTed block. Zero means unlimited. Throw exception if the block contains too many partitions. This setting is a safety threshold, because using large number of partitions is a common misconception.", 0) \
    M(Bool, throw_on_max_partitions_per_insert_block, true, "Used with max_partitions_per_insert_block. If true (default), an exception will be thrown when max_partitions_per_insert_block is reached. If false, details of the insert query reaching this limit with the number of partitions will be logged. This can be useful if you're trying to understand the impact on users when changing max_partitions_per_insert_block.", 0) \
    M(Int64, max_partitions_to_read, -1, "Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited.", 0) \
    M(Bool, check_query_single_value_result, true, "Return check query result as single 1/0 value", 0) \
    M(Bool, allow_drop_detached, false, "Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries", 0) \
    \
    M(UInt64, postgresql_connection_pool_size, 16, "Connection pool size for PostgreSQL table engine and database engine.", 0) \
    M(UInt64, postgresql_connection_pool_wait_timeout, 5000, "Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty pool.", 0) \
    M(Bool, postgresql_connection_pool_auto_close_connection, false, "Close connection before returning connection to the pool.", 0) \
    M(UInt64, glob_expansion_max_elements, 1000, "Maximum number of allowed addresses (For external storages, table functions, etc).", 0) \
    M(UInt64, odbc_bridge_connection_pool_size, 16, "Connection pool size for each connection settings string in ODBC bridge.", 0) \
    M(Bool, odbc_bridge_use_connection_pooling, true, "Use connection pooling in ODBC bridge. If set to false, a new connection is created every time", 0) \
    \
    M(Seconds, distributed_replica_error_half_life, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD, "Time period reduces replica error counter by 2 times.", 0) \
    M(UInt64, distributed_replica_error_cap, DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT, "Max number of errors per replica, prevents piling up an incredible amount of errors if replica was offline for some time and allows it to be reconsidered in a shorter amount of time.", 0) \
    M(UInt64, distributed_replica_max_ignored_errors, 0, "Number of errors that will be ignored while choosing replicas", 0) \
    \
    M(Bool, allow_experimental_live_view, false, "Enable LIVE VIEW. Not mature enough.", 0) \
    M(Seconds, live_view_heartbeat_interval, 15, "The heartbeat interval in seconds to indicate live query is alive.", 0) \
    M(UInt64, max_live_view_insert_blocks_before_refresh, 64, "Limit maximum number of inserted blocks after which mergeable blocks are dropped and query is re-executed.", 0) \
    M(Bool, allow_experimental_window_view, false, "Enable WINDOW VIEW. Not mature enough.", 0) \
    M(Seconds, window_view_clean_interval, 60, "The clean interval of window view in seconds to free outdated data.", 0) \
    M(Seconds, window_view_heartbeat_interval, 15, "The heartbeat interval in seconds to indicate watch query is alive.", 0) \
    M(Seconds, wait_for_window_view_fire_signal_timeout, 10, "Timeout for waiting for window view fire signal in event time processing", 0) \
    M(UInt64, min_free_disk_space_for_temporary_data, 0, "The minimum disk space to keep while writing temporary data used in external sorting and aggregation.", 0) \
    \
    M(DefaultTableEngine, default_temporary_table_engine, DefaultTableEngine::Memory, "Default table engine used when ENGINE is not set in CREATE TEMPORARY statement.",0) \
    M(DefaultTableEngine, default_table_engine, DefaultTableEngine::None, "Default table engine used when ENGINE is not set in CREATE statement.",0) \
    M(Bool, show_table_uuid_in_table_create_query_if_not_nil, false, "For tables in databases with Engine=Atomic show UUID of the table in its CREATE query.", 0) \
    M(Bool, database_atomic_wait_for_drop_and_detach_synchronously, false, "When executing DROP or DETACH TABLE in Atomic database, wait for table data to be finally dropped or detached.", 0) \
    M(Bool, enable_scalar_subquery_optimization, true, "If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.", 0) \
    M(Bool, optimize_trivial_count_query, true, "Process trivial 'SELECT count() FROM table' query from metadata.", 0) \
    M(Bool, optimize_count_from_files, true, "Optimize counting rows from files in supported input formats", 0) \
    M(Bool, use_cache_for_count_from_files, true, "Use cache to count the number of rows in files", 0) \
    M(Bool, optimize_respect_aliases, true, "If it is set to true, it will respect aliases in WHERE/GROUP BY/ORDER BY, that will help with partition pruning/secondary indexes/optimize_aggregation_in_order/optimize_read_in_order/optimize_trivial_count", 0) \
    M(UInt64, mutations_sync, 0, "Wait for synchronous execution of ALTER TABLE UPDATE/DELETE queries (mutations). 0 - execute asynchronously. 1 - wait current server. 2 - wait all replicas if they exist.", 0) \
    M(Bool, enable_lightweight_delete, true, "Enable lightweight DELETE mutations for mergetree tables.", 0) ALIAS(allow_experimental_lightweight_delete) \
    M(Bool, optimize_move_functions_out_of_any, false, "Move functions out of aggregate functions 'any', 'anyLast'.", 0) \
    M(Bool, optimize_normalize_count_variants, true, "Rewrite aggregate functions that semantically equals to count() as count().", 0) \
    M(Bool, optimize_injective_functions_inside_uniq, true, "Delete injective functions of one argument inside uniq*() functions.", 0) \
    M(Bool, rewrite_count_distinct_if_with_count_distinct_implementation, false, "Rewrite countDistinctIf with count_distinct_implementation configuration", 0) \
    M(Bool, convert_query_to_cnf, false, "Convert SELECT query to CNF", 0) \
    M(Bool, optimize_or_like_chain, false, "Optimize multiple OR LIKE into multiMatchAny. This optimization should not be enabled by default, because it defies index analysis in some cases.", 0) \
    M(Bool, optimize_arithmetic_operations_in_aggregate_functions, true, "Move arithmetic operations out of aggregation functions", 0) \
    M(Bool, optimize_redundant_functions_in_order_by, true, "Remove functions from ORDER BY if its argument is also in ORDER BY", 0) \
    M(Bool, optimize_if_chain_to_multiif, false, "Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not beneficial for numeric types.", 0) \
    M(Bool, optimize_multiif_to_if, true, "Replace 'multiIf' with only one condition to 'if'.", 0) \
    M(Bool, optimize_if_transform_strings_to_enum, false, "Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in distributed query that would lead to its fail.", 0) \
    M(Bool, optimize_monotonous_functions_in_order_by, false, "Replace monotonous function with its argument in ORDER BY", 0) \
    M(Bool, optimize_functions_to_subcolumns, false, "Transform functions to subcolumns, if possible, to reduce amount of read data. E.g. 'length(arr)' -> 'arr.size0', 'col IS NULL' -> 'col.null' ", 0) \
    M(Bool, optimize_using_constraints, false, "Use constraints for query optimization", 0)                                                                                                                                           \
    M(Bool, optimize_substitute_columns, false, "Use constraints for column substitution", 0)                                                                                                                                         \
    M(Bool, optimize_append_index, false, "Use constraints in order to append index condition (indexHint)", 0) \
    M(Bool, normalize_function_names, true, "Normalize function names to their canonical names", 0) \
    M(Bool, allow_experimental_alter_materialized_view_structure, false, "Allow atomic alter on Materialized views. Work in progress.", 0) \
    M(Bool, enable_early_constant_folding, true, "Enable query optimization where we analyze function and subqueries results and rewrite query if there're constants there", 0) \
    M(Bool, deduplicate_blocks_in_dependent_materialized_views, false, "Should deduplicate blocks for materialized views if the block is not a duplicate for the table. Use true to always deduplicate in dependent tables.", 0) \
    M(Bool, materialized_views_ignore_errors, false, "Allows to ignore errors for MATERIALIZED VIEW, and deliver original block to the table regardless of MVs", 0) \
    M(Bool, use_compact_format_in_distributed_parts_names, true, "Changes format of directories names for distributed table insert parts.", 0) \
    M(Bool, validate_polygons, true, "Throw exception if polygon is invalid in function pointInPolygon (e.g. self-tangent, self-intersecting). If the setting is false, the function will accept invalid polygons but may silently return wrong result.", 0) \
    M(UInt64, max_parser_depth, DBMS_DEFAULT_MAX_PARSER_DEPTH, "Maximum parser depth (recursion depth of recursive descend parser).", 0) \
    M(Bool, allow_settings_after_format_in_insert, false, "Allow SETTINGS after FORMAT, but note, that this is not always safe (note: this is a compatibility setting).", 0) \
    M(Seconds, periodic_live_view_refresh, DEFAULT_PERIODIC_LIVE_VIEW_REFRESH_SEC, "Interval after which periodically refreshed live view is forced to refresh.", 0) \
    M(Bool, transform_null_in, false, "If enabled, NULL values will be matched with 'IN' operator as if they are considered equal.", 0) \
    M(Bool, allow_nondeterministic_mutations, false, "Allow non-deterministic functions in ALTER UPDATE/ALTER DELETE statements", 0) \
    M(Seconds, lock_acquire_timeout, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "How long locking request should wait before failing", 0) \
    M(Bool, materialize_ttl_after_modify, true, "Apply TTL for old data, after ALTER MODIFY TTL query", 0) \
    M(String, function_implementation, "", "Choose function implementation for specific target or variant (experimental). If empty enable all of them.", 0) \
    M(Bool, data_type_default_nullable, false, "Data types without NULL or NOT NULL will make Nullable", 0) \
    M(Bool, cast_keep_nullable, false, "CAST operator keep Nullable for result data type", 0) \
    M(Bool, cast_ipv4_ipv6_default_on_conversion_error, false, "CAST operator into IPv4, CAST operator into IPV6 type, toIPv4, toIPv6 functions will return default value instead of throwing exception on conversion error.", 0) \
    M(Bool, alter_partition_verbose_result, false, "Output information about affected parts. Currently works only for FREEZE and ATTACH commands.", 0) \
    M(Bool, allow_experimental_database_materialized_mysql, false, "Allow to create database with Engine=MaterializedMySQL(...).", 0) \
    M(Bool, allow_experimental_database_materialized_postgresql, false, "Allow to create database with Engine=MaterializedPostgreSQL(...).", 0) \
    M(Bool, system_events_show_zero_values, false, "When querying system.events or system.metrics tables, include all metrics, even with zero values.", 0) \
    M(MySQLDataTypesSupport, mysql_datatypes_support_level, 0, "Which MySQL types should be converted to corresponding ClickHouse types (rather than being represented as String). Can be empty or any combination of 'decimal', 'datetime64', 'date2Date32' or 'date2String'. When empty MySQL's DECIMAL and DATETIME/TIMESTAMP with non-zero precision are seen as String on ClickHouse's side.", 0) \
    M(Bool, optimize_trivial_insert_select, true, "Optimize trivial 'INSERT INTO table SELECT ... FROM TABLES' query", 0) \
    M(Bool, allow_non_metadata_alters, true, "Allow to execute alters which affects not only tables metadata, but also data on disk", 0) \
    M(Bool, enable_global_with_statement, true, "Propagate WITH statements to UNION queries and all subqueries", 0) \
    M(Bool, aggregate_functions_null_for_empty, false, "Rewrite all aggregate functions in a query, adding -OrNull suffix to them", 0) \
    M(Bool, optimize_syntax_fuse_functions, false, "Allow apply fuse aggregating function. Available only with `allow_experimental_analyzer`", 0) \
    M(Bool, flatten_nested, true, "If true, columns of type Nested will be flatten to separate array columns instead of one array of tuples", 0) \
    M(Bool, asterisk_include_materialized_columns, false, "Include MATERIALIZED columns for wildcard query", 0) \
    M(Bool, asterisk_include_alias_columns, false, "Include ALIAS columns for wildcard query", 0) \
    M(Bool, optimize_skip_merged_partitions, false, "Skip partitions with one part with level > 0 in optimize final", 0) \
    M(Bool, optimize_on_insert, true, "Do the same transformation for inserted block of data as if merge was done on this block.", 0) \
    M(Bool, optimize_use_projections, true, "Automatically choose projections to perform SELECT query", 0) ALIAS(allow_experimental_projection_optimization) \
    M(Bool, optimize_use_implicit_projections, true, "Automatically choose implicit projections to perform SELECT query", 0) \
    M(Bool, force_optimize_projection, false, "If projection optimization is enabled, SELECT queries need to use projection", 0) \
    M(Bool, async_socket_for_remote, true, "Asynchronously read from socket executing remote query", 0) \
    M(Bool, async_query_sending_for_remote, true, "Asynchronously create connections and send query to shards in remote query", 0) \
    M(Bool, insert_null_as_default, true, "Insert DEFAULT values instead of NULL in INSERT SELECT (UNION ALL)", 0) \
    M(Bool, describe_extend_object_types, false, "Deduce concrete type of columns of type Object in DESCRIBE query", 0) \
    M(Bool, describe_include_subcolumns, false, "If true, subcolumns of all table columns will be included into result of DESCRIBE query", 0) \
    M(Bool, mutations_execute_nondeterministic_on_initiator, false, "If true nondeterministic function are executed on initiator and replaced to literals in UPDATE and DELETE queries", 0) \
    M(Bool, mutations_execute_subqueries_on_initiator, false, "If true scalar subqueries are executed on initiator and replaced to literals in UPDATE and DELETE queries", 0) \
    M(UInt64, mutations_max_literal_size_to_replace, 16384, "The maximum size of serialized literal in bytes to replace in UPDATE and DELETE queries", 0) \
    \
    M(Bool, use_query_cache, false, "Enable the query cache", 0) \
    M(Bool, enable_writes_to_query_cache, true, "Enable storing results of SELECT queries in the query cache", 0) \
    M(Bool, enable_reads_from_query_cache, true, "Enable reading results of SELECT queries from the query cache", 0) \
    M(Bool, query_cache_store_results_of_queries_with_nondeterministic_functions, false, "Store results of queries with non-deterministic functions (e.g. rand(), now()) in the query cache", 0) \
    M(UInt64, query_cache_max_size_in_bytes, 0, "The maximum amount of memory (in bytes) the current user may allocate in the query cache. 0 means unlimited. ", 0) \
    M(UInt64, query_cache_max_entries, 0, "The maximum number of query results the current user may store in the query cache. 0 means unlimited.", 0) \
    M(UInt64, query_cache_min_query_runs, 0, "Minimum number a SELECT query must run before its result is stored in the query cache", 0) \
    M(Milliseconds, query_cache_min_query_duration, 0, "Minimum time in milliseconds for a query to run for its result to be stored in the query cache.", 0) \
    M(Bool, query_cache_compress_entries, true, "Compress cache entries.", 0) \
    M(Bool, query_cache_squash_partial_results, true, "Squash partial result blocks to blocks of size 'max_block_size'. Reduces performance of inserts into the query cache but improves the compressability of cache entries.", 0) \
    M(Seconds, query_cache_ttl, 60, "After this time in seconds entries in the query cache become stale", 0) \
    M(Bool, query_cache_share_between_users, false, "Allow other users to read entry in the query cache", 0) \
    M(Bool, enable_sharing_sets_for_mutations, true, "Allow sharing set objects build for IN subqueries between different tasks of the same mutation. This reduces memory usage and CPU consumption", 0) \
    \
    M(Bool, optimize_rewrite_sum_if_to_count_if, false, "Rewrite sumIf() and sum(if()) function countIf() function when logically equivalent", 0) \
    M(Bool, optimize_rewrite_aggregate_function_with_if, true, "Rewrite aggregate functions with if expression as argument when logically equivalent. For example, avg(if(cond, col, null)) can be rewritten to avgIf(cond, col)", 0) \
    M(Bool, optimize_rewrite_array_exists_to_has, true, "Rewrite arrayExists() functions to has() when logically equivalent. For example, arrayExists(x -> x = 1, arr) can be rewritten to has(arr, 1)", 0) \
    M(UInt64, insert_shard_id, 0, "If non zero, when insert into a distributed table, the data will be inserted into the shard `insert_shard_id` synchronously. Possible values range from 1 to `shards_number` of corresponding distributed table", 0) \
    \
    M(Bool, collect_hash_table_stats_during_aggregation, true, "Enable collecting hash table statistics to optimize memory allocation", 0) \
    M(UInt64, max_entries_for_hash_table_stats, 10'000, "How many entries hash table statistics collected during aggregation is allowed to have", 0) \
    M(UInt64, max_size_to_preallocate_for_aggregation, 100'000'000, "For how many elements it is allowed to preallocate space in all hash tables in total before aggregation", 0) \
    \
    M(Bool, kafka_disable_num_consumers_limit, false, "Disable limit on kafka_num_consumers that depends on the number of available CPU cores", 0) \
    M(Bool, enable_software_prefetch_in_aggregation, true, "Enable use of software prefetch in aggregation", 0) \
    M(Bool, allow_aggregate_partitions_independently, false, "Enable independent aggregation of partitions on separate threads when partition key suits group by key. Beneficial when number of partitions close to number of cores and partitions have roughly the same size", 0) \
    M(Bool, force_aggregate_partitions_independently, false, "Force the use of optimization when it is applicable, but heuristics decided not to use it", 0) \
    M(UInt64, max_number_of_partitions_for_independent_aggregation, 128, "Maximal number of partitions in table to apply optimization", 0) \
    /** Experimental feature for moving data between shards. */ \
    \
    M(Bool, allow_experimental_query_deduplication, false, "Experimental data deduplication for SELECT queries based on part UUIDs", 0) \
    \
    M(Bool, engine_file_empty_if_not_exists, false, "Allows to select data from a file engine table without file", 0) \
    M(Bool, engine_file_truncate_on_insert, false, "Enables or disables truncate before insert in file engine tables", 0) \
    M(Bool, engine_file_allow_create_multiple_files, false, "Enables or disables creating a new file on each insert in file engine tables if format has suffix.", 0) \
    M(Bool, engine_file_skip_empty_files, false, "Allows to skip empty files in file table engine", 0) \
    M(Bool, engine_url_skip_empty_files, false, "Allows to skip empty files in url table engine", 0) \
    M(Bool, enable_url_encoding, true, " Allows to enable/disable decoding/encoding path in uri in URL table engine", 0) \
    M(Bool, allow_experimental_database_replicated, false, "Allow to create databases with Replicated engine", 0) \
    M(UInt64, database_replicated_initial_query_timeout_sec, 300, "How long initial DDL query should wait for Replicated database to precess previous DDL queue entries", 0) \
    M(Bool, database_replicated_enforce_synchronous_settings, false, "Enforces synchronous waiting for some queries (see also database_atomic_wait_for_drop_and_detach_synchronously, mutation_sync, alter_sync). Not recommended to enable these settings.", 0) \
    M(UInt64, max_distributed_depth, 5, "Maximum distributed query depth", 0) \
    M(Bool, database_replicated_always_detach_permanently, false, "Execute DETACH TABLE as DETACH TABLE PERMANENTLY if database engine is Replicated", 0) \
    M(Bool, database_replicated_allow_only_replicated_engine, false, "Allow to create only Replicated tables in database with engine Replicated", 0) \
    M(Bool, database_replicated_allow_replicated_engine_arguments, true, "Allow to create only Replicated tables in database with engine Replicated with explicit arguments", 0) \
    M(DistributedDDLOutputMode, distributed_ddl_output_mode, DistributedDDLOutputMode::THROW, "Format of distributed DDL query result, one of: 'none', 'throw', 'null_status_on_timeout', 'never_throw'", 0) \
    M(UInt64, distributed_ddl_entry_format_version, 5, "Compatibility version of distributed DDL (ON CLUSTER) queries", 0) \
    \
    M(UInt64, external_storage_max_read_rows, 0, "Limit maximum number of rows when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled", 0) \
    M(UInt64, external_storage_max_read_bytes, 0, "Limit maximum number of bytes when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled", 0)  \
    M(UInt64, external_storage_connect_timeout_sec, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connect timeout in seconds. Now supported only for MySQL", 0)  \
    M(UInt64, external_storage_rw_timeout_sec, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "Read/write timeout in seconds. Now supported only for MySQL", 0)  \
    \
    M(SetOperationMode, union_default_mode, SetOperationMode::Unspecified, "Set default mode in UNION query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.", 0) \
    M(SetOperationMode, intersect_default_mode, SetOperationMode::ALL, "Set default mode in INTERSECT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.", 0) \
    M(SetOperationMode, except_default_mode, SetOperationMode::ALL, "Set default mode in EXCEPT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.", 0) \
    M(Bool, optimize_aggregators_of_group_by_keys, true, "Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section", 0) \
    M(Bool, optimize_group_by_function_keys, true, "Eliminates functions of other keys in GROUP BY section", 0) \
    M(Bool, legacy_column_name_of_tuple_literal, false, "List all names of element of large tuple literals in their column names instead of hash. This settings exists only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher.", 0) \
    \
    M(Bool, query_plan_enable_optimizations, true, "Apply optimizations to query plan", 0) \
    M(UInt64, query_plan_max_optimizations_to_apply, 10000, "Limit the total number of optimizations applied to query plan. If zero, ignored. If limit reached, throw exception", 0) \
    M(Bool, query_plan_filter_push_down, true, "Allow to push down filter by predicate query plan step", 0) \
    M(Bool, query_plan_optimize_primary_key, true, "Analyze primary key using query plan (instead of AST)", 0) \
    M(Bool, query_plan_read_in_order, true, "Use query plan for read-in-order optimisation", 0) \
    M(Bool, query_plan_aggregation_in_order, true, "Use query plan for aggregation-in-order optimisation", 0) \
    M(Bool, query_plan_remove_redundant_sorting, true, "Remove redundant sorting in query plan. For example, sorting steps related to ORDER BY clauses in subqueries", 0) \
    M(Bool, query_plan_remove_redundant_distinct, true, "Remove redundant Distinct step in query plan", 0) \
    M(Bool, query_plan_optimize_projection, true, "Use query plan for aggregation-in-order optimisation", 0) \
    M(UInt64, regexp_max_matches_per_row, 1000, "Max matches of any single regexp per row, used to safeguard 'extractAllGroupsHorizontal' against consuming too much memory with greedy RE.", 0) \
    \
    M(UInt64, limit, 0, "Limit on read rows from the most 'end' result for select query, default 0 means no limit length", 0) \
    M(UInt64, offset, 0, "Offset on read rows from the most 'end' result for select query", 0) \
    \
    M(UInt64, function_range_max_elements_in_block, 500000000, "Maximum number of values generated by function `range` per block of data (sum of array sizes for every row in a block, see also 'max_block_size' and 'min_insert_block_size_rows'). It is a safety threshold.", 0) \
    M(UInt64, function_sleep_max_microseconds_per_block, 3000000, "Maximum number of microseconds the function `sleep` is allowed to sleep for each block. If a user called it with a larger value, it throws an exception. It is a safety threshold.", 0) \
    M(ShortCircuitFunctionEvaluation, short_circuit_function_evaluation, ShortCircuitFunctionEvaluation::ENABLE, "Setting for short-circuit function evaluation configuration. Possible values: 'enable' - use short-circuit function evaluation for functions that are suitable for it, 'disable' - disable short-circuit function evaluation, 'force_enable' - use short-circuit function evaluation for all functions.", 0) \
    \
    M(LocalFSReadMethod, storage_file_read_method, LocalFSReadMethod::pread, "Method of reading data from storage file, one of: read, pread, mmap. The mmap method does not apply to clickhouse-server (it's intended for clickhouse-local).", 0) \
    M(String, local_filesystem_read_method, "pread_threadpool", "Method of reading data from local filesystem, one of: read, pread, mmap, io_uring, pread_threadpool. The 'io_uring' method is experimental and does not work for Log, TinyLog, StripeLog, File, Set and Join, and other tables with append-able files in presence of concurrent reads and writes.", 0) \
    M(String, remote_filesystem_read_method, "threadpool", "Method of reading data from remote filesystem, one of: read, threadpool.", 0) \
    M(Bool, local_filesystem_read_prefetch, false, "Should use prefetching when reading data from local filesystem.", 0) \
    M(Bool, remote_filesystem_read_prefetch, true, "Should use prefetching when reading data from remote filesystem.", 0) \
    M(Int64, read_priority, 0, "Priority to read data from local filesystem or remote filesystem. Only supported for 'pread_threadpool' method for local filesystem and for `threadpool` method for remote filesystem.", 0) \
    M(UInt64, merge_tree_min_rows_for_concurrent_read_for_remote_filesystem, (20 * 8192), "If at least as many lines are read from one file, the reading can be parallelized, when reading from remote filesystem.", 0) \
    M(UInt64, merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem, (24 * 10 * 1024 * 1024), "If at least as many bytes are read from one file, the reading can be parallelized, when reading from remote filesystem.", 0) \
    M(UInt64, remote_read_min_bytes_for_seek, 4 * DBMS_DEFAULT_BUFFER_SIZE, "Min bytes required for remote read (url, s3) to do seek, instead of read with ignore.", 0) \
    M(UInt64, merge_tree_min_bytes_per_task_for_remote_reading, 4 * DBMS_DEFAULT_BUFFER_SIZE, "Min bytes to read per task.", 0) \
    M(Bool, merge_tree_use_const_size_tasks_for_remote_reading, true, "Whether to use constant size tasks for reading from a remote table.", 0) \
    M(Bool, merge_tree_determine_task_size_by_prewhere_columns, true, "Whether to use only prewhere columns size to determine reading task size.", 0) \
    \
    M(Bool, async_insert, false, "If true, data from INSERT query is stored in queue and later flushed to table in background. If wait_for_async_insert is false, INSERT query is processed almost instantly, otherwise client will wait until data will be flushed to table", 0) \
    M(Bool, wait_for_async_insert, true, "If true wait for processing of asynchronous insertion", 0) \
    M(Seconds, wait_for_async_insert_timeout, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "Timeout for waiting for processing asynchronous insertion", 0) \
    M(UInt64, async_insert_max_data_size, 1000000, "Maximum size in bytes of unparsed data collected per query before being inserted", 0) \
    M(UInt64, async_insert_max_query_number, 450, "Maximum number of insert queries before being inserted", 0) \
    M(Milliseconds, async_insert_busy_timeout_ms, 200, "Maximum time to wait before dumping collected data per query since the first data appeared", 0) \
    \
    M(UInt64, remote_fs_read_max_backoff_ms, 10000, "Max wait time when trying to read data for remote disk", 0) \
    M(UInt64, remote_fs_read_backoff_max_tries, 5, "Max attempts to read with backoff", 0) \
    M(Bool, enable_filesystem_cache, true, "Use cache for remote filesystem. This setting does not turn on/off cache for disks (must be done via disk config), but allows to bypass cache for some queries if intended", 0) \
    M(Bool, enable_filesystem_cache_on_write_operations, false, "Write into cache on write operations. To actually work this setting requires be added to disk config too", 0) \
    M(Bool, enable_filesystem_cache_log, false, "Allows to record the filesystem caching log for each query", 0) \
    M(Bool, read_from_filesystem_cache_if_exists_otherwise_bypass_cache, false, "Allow to use the filesystem cache in passive mode - benefit from the existing cache entries, but don't put more entries into the cache. If you set this setting for heavy ad-hoc queries and leave it disabled for short real-time queries, this will allows to avoid cache threshing by too heavy queries and to improve the overall system efficiency.", 0) \
    M(Bool, skip_download_if_exceeds_query_cache, true, "Skip download from remote filesystem if exceeds query cache size", 0) \
    M(UInt64, filesystem_cache_max_download_size, (128UL * 1024 * 1024 * 1024), "Max remote filesystem cache size that can be downloaded by a single query", 0) \
    M(Bool, throw_on_error_from_cache_on_write_operations, false, "Ignore error from cache when caching on write operations (INSERT, merges)", 0) \
    \
    M(Bool, load_marks_asynchronously, false, "Load MergeTree marks asynchronously", 0) \
    M(Bool, enable_filesystem_read_prefetches_log, false, "Log to system.filesystem prefetch_log during query. Should be used only for testing or debugging, not recommended to be turned on by default", 0) \
    M(Bool, allow_prefetched_read_pool_for_remote_filesystem, false, "Prefer prefethed threadpool if all parts are on remote filesystem", 0) \
    M(Bool, allow_prefetched_read_pool_for_local_filesystem, false, "Prefer prefethed threadpool if all parts are on remote filesystem", 0) \
    \
    M(UInt64, prefetch_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, "The maximum size of the prefetch buffer to read from the filesystem.", 0) \
    M(UInt64, filesystem_prefetch_step_bytes, 0, "Prefetch step in bytes. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task", 0) \
    M(UInt64, filesystem_prefetch_step_marks, 0, "Prefetch step in marks. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task", 0) \
    M(UInt64, filesystem_prefetch_min_bytes_for_single_read_task, "8Mi", "Do not parallelize within one file read less than this amount of bytes. E.g. one reader will not receive a read task of size less than this amount. This setting is recommended to avoid spikes of time for aws getObject requests to aws", 0) \
    M(UInt64, filesystem_prefetch_max_memory_usage, "1Gi", "Maximum memory usage for prefetches. Zero means unlimited", 0) \
    M(UInt64, filesystem_prefetches_limit, 0, "Maximum number of prefetches. Zero means unlimited. A setting `filesystem_prefetches_max_memory_usage` is more recommended if you want to limit the number of prefetches", 0) \
    \
    M(UInt64, use_structure_from_insertion_table_in_table_functions, 2, "Use structure from insertion table instead of schema inference from data. Possible values: 0 - disabled, 1 - enabled, 2 - auto", 0) \
    \
    M(UInt64, http_max_tries, 10, "Max attempts to read via http.", 0) \
    M(UInt64, http_retry_initial_backoff_ms, 100, "Min milliseconds for backoff, when retrying read via http", 0) \
    M(UInt64, http_retry_max_backoff_ms, 10000, "Max milliseconds for backoff, when retrying read via http", 0) \
    \
    M(Bool, force_remove_data_recursively_on_drop, false, "Recursively remove data on DROP query. Avoids 'Directory not empty' error, but may silently remove detached data", 0) \
    M(Bool, check_table_dependencies, true, "Check that DDL query (such as DROP TABLE or RENAME) will not break dependencies", 0) \
    M(Bool, check_referential_table_dependencies, false, "Check that DDL query (such as DROP TABLE or RENAME) will not break referential dependencies", 0) \
    M(Bool, use_local_cache_for_remote_storage, true, "Use local cache for remote storage like HDFS or S3, it's used for remote table engine only", 0) \
    \
    M(Bool, allow_unrestricted_reads_from_keeper, false, "Allow unrestricted (without condition on path) reads from system.zookeeper table, can be handy, but is not safe for zookeeper", 0) \
    M(Bool, allow_deprecated_database_ordinary, false, "Allow to create databases with deprecated Ordinary engine", 0) \
    M(Bool, allow_deprecated_syntax_for_merge_tree, false, "Allow to create *MergeTree tables with deprecated engine definition syntax", 0) \
    M(Bool, allow_asynchronous_read_from_io_pool_for_merge_tree, false, "Use background I/O pool to read from MergeTree tables. This setting may increase performance for I/O bound queries", 0) \
    M(UInt64, max_streams_for_merge_tree_reading, 0, "If is not zero, limit the number of reading streams for MergeTree table.", 0) \
    \
    M(Bool, force_grouping_standard_compatibility, true, "Make GROUPING function to return 1 when argument is not used as an aggregation key", 0) \
    \
    M(Bool, schema_inference_use_cache_for_file, true, "Use cache in schema inference while using file table function", 0) \
    M(Bool, schema_inference_use_cache_for_s3, true, "Use cache in schema inference while using s3 table function", 0) \
    M(Bool, schema_inference_use_cache_for_azure, true, "Use cache in schema inference while using azure table function", 0) \
    M(Bool, schema_inference_use_cache_for_hdfs, true, "Use cache in schema inference while using hdfs table function", 0) \
    M(Bool, schema_inference_use_cache_for_url, true, "Use cache in schema inference while using url table function", 0) \
    M(Bool, schema_inference_cache_require_modification_time_for_url, true, "Use schema from cache for URL with last modification time validation (for urls with Last-Modified header)", 0) \
    \
    M(String, compatibility, "", "Changes other settings according to provided ClickHouse version. If we know that we changed some behaviour in ClickHouse by changing some settings in some version, this compatibility setting will control these settings", 0) \
    \
    M(Map, additional_table_filters, "", "Additional filter expression which would be applied after reading from specified table. Syntax: {'table1': 'expression', 'database.table2': 'expression'}", 0) \
    M(String, additional_result_filter, "", "Additional filter expression which would be applied to query result", 0) \
    \
    M(String, workload, "default", "Name of workload to be used to access resources", 0) \
    M(Milliseconds, storage_system_stack_trace_pipe_read_timeout_ms, 100, "Maximum time to read from a pipe for receiving information from the threads when querying the `system.stack_trace` table. This setting is used for testing purposes and not meant to be changed by users.", 0) \
    \
    M(String, rename_files_after_processing, "", "Rename successfully processed files according to the specified pattern; Pattern can include the following placeholders: `%a` (full original file name), `%f` (original filename without extension), `%e` (file extension with dot), `%t` (current timestamp in µs), and `%%` (% sign)", 0) \
    \
    M(Bool, parallelize_output_from_storages, true, "Parallelize output for reading step from storage. It allows parallelizing query processing right after reading from storage if possible", 0) \
    M(String, insert_deduplication_token, "", "If not empty, used for duplicate detection instead of data digest", 0) \
    M(Bool, count_distinct_optimization, false, "Rewrite count distinct to subquery of group by", 0) \
    M(Bool, throw_if_no_data_to_insert, true, "Enables or disables empty INSERTs, enabled by default", 0) \
    M(Bool, compatibility_ignore_auto_increment_in_create_table, false, "Ignore AUTO_INCREMENT keyword in column declaration if true, otherwise return error. It simplifies migration from MySQL", 0) \
    M(Bool, multiple_joins_try_to_keep_original_names, false, "Do not add aliases to top level expression list on multiple joins rewrite", 0) \
    M(Bool, optimize_sorting_by_input_stream_properties, true, "Optimize sorting by sorting properties of input stream", 0) \
    M(UInt64, insert_keeper_max_retries, 20, "Max retries for keeper operations during insert", 0) \
    M(UInt64, insert_keeper_retry_initial_backoff_ms, 100, "Initial backoff timeout for keeper operations during insert", 0) \
    M(UInt64, insert_keeper_retry_max_backoff_ms, 10000, "Max backoff timeout for keeper operations during insert", 0) \
    M(Float, insert_keeper_fault_injection_probability, 0.0f, "Approximate probability of failure for a keeper request during insert. Valid value is in interval [0.0f, 1.0f]", 0) \
    M(UInt64, insert_keeper_fault_injection_seed, 0, "0 - random seed, otherwise the setting value", 0) \
    M(Bool, force_aggregation_in_order, false, "Force use of aggregation in order on remote nodes during distributed aggregation. PLEASE, NEVER CHANGE THIS SETTING VALUE MANUALLY!", IMPORTANT) \
    M(UInt64, http_max_request_param_data_size, 10_MiB, "Limit on size of request data used as a query parameter in predefined HTTP requests.", 0) \
    M(Bool, function_json_value_return_type_allow_nullable, false, "Allow function JSON_VALUE to return nullable type.", 0) \
    M(Bool, function_json_value_return_type_allow_complex, false, "Allow function JSON_VALUE to return complex type, such as: struct, array, map.", 0) \
    M(Bool, use_with_fill_by_sorting_prefix, true, "Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently", 0) \
    \
    /** Experimental functions */ \
    M(Bool, allow_experimental_funnel_functions, false, "Enable experimental functions for funnel analysis.", 0) \
    M(Bool, allow_experimental_nlp_functions, false, "Enable experimental functions for natural language processing.", 0) \
    M(Bool, allow_experimental_hash_functions, false, "Enable experimental hash functions", 0) \
    M(Bool, allow_experimental_object_type, false, "Allow Object and JSON data types", 0) \
    M(Bool, allow_experimental_annoy_index, false, "Allows to use Annoy index. Disabled by default because this feature is experimental", 0) \
    M(Bool, allow_experimental_usearch_index, false, "Allows to use USearch index. Disabled by default because this feature is experimental", 0) \
    M(UInt64, max_limit_for_ann_queries, 1'000'000, "SELECT queries with LIMIT bigger than this setting cannot use ANN indexes. Helps to prevent memory overflows in ANN search indexes.", 0) \
    M(Int64, annoy_index_search_k_nodes, -1, "SELECT queries search up to this many nodes in Annoy indexes.", 0) \
    M(Bool, throw_on_unsupported_query_inside_transaction, true, "Throw exception if unsupported query is used inside transaction", 0) \
    M(TransactionsWaitCSNMode, wait_changes_become_visible_after_commit_mode, TransactionsWaitCSNMode::WAIT_UNKNOWN, "Wait for committed changes to become actually visible in the latest snapshot", 0) \
    M(Bool, implicit_transaction, false, "If enabled and not already inside a transaction, wraps the query inside a full transaction (begin + commit or rollback)", 0) \
    M(UInt64, grace_hash_join_initial_buckets, 1, "Initial number of grace hash join buckets", 0) \
    M(UInt64, grace_hash_join_max_buckets, 1024, "Limit on the number of grace hash join buckets", 0) \
    M(Bool, optimize_distinct_in_order, true, "Enable DISTINCT optimization if some columns in DISTINCT form a prefix of sorting. For example, prefix of sorting key in merge tree or ORDER BY statement", 0) \
    M(Bool, allow_experimental_undrop_table_query, false, "Allow to use undrop query to restore dropped table in a limited time", 0) \
    M(Bool, keeper_map_strict_mode, false, "Enforce additional checks during operations on KeeperMap. E.g. throw an exception on an insert for already existing key", 0) \
    M(UInt64, extract_kvp_max_pairs_per_row, 1000, "Max number pairs that can be produced by extractKeyValuePairs function. Used to safeguard against consuming too much memory.", 0) \
    M(Timezone, session_timezone, "", "This setting can be removed in the future due to potential caveats. It is experimental and is not suitable for production usage. The default timezone for current session or query. The server default timezone if empty.", 0) \
    M(Bool, allow_create_index_without_type, false, "Allow CREATE INDEX query without TYPE. Query will be ignored. Made for SQL compatibility tests.", 0)\
    M(Bool, create_index_ignore_unique, false, "Ignore UNIQUE keyword in CREATE UNIQUE INDEX. Made for SQL compatibility tests.", 0)\
    // End of COMMON_SETTINGS
    // Please add settings related to formats into the FORMAT_FACTORY_SETTINGS and move obsolete settings to OBSOLETE_SETTINGS.

#define MAKE_OBSOLETE(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", BaseSettingsHelpers::Flags::OBSOLETE)

/// NOTE: ServerSettings::loadSettingsFromConfig() should be updated to include this settings
#define MAKE_DEPRECATED_BY_SERVER_CONFIG(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "User-level setting is deprecated, and it must be defined in the server configuration instead.", BaseSettingsHelpers::Flags::OBSOLETE)

#define OBSOLETE_SETTINGS(M, ALIAS) \
    /** Obsolete settings that do nothing but left for compatibility reasons. Remove each one after half a year of obsolescence. */ \
    MAKE_OBSOLETE(M, UInt64, max_memory_usage_for_all_queries, 0) \
    MAKE_OBSOLETE(M, UInt64, multiple_joins_rewriter_version, 0) \
    MAKE_OBSOLETE(M, Bool, enable_debug_queries, false) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_database_atomic, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_bigint_types, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_window_functions, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_geo_types, true) \
    \
    MAKE_OBSOLETE(M, Milliseconds, async_insert_stale_timeout_ms, 0) \
    MAKE_OBSOLETE(M, HandleKafkaErrorMode, handle_kafka_error_mode, HandleKafkaErrorMode::DEFAULT) \
    MAKE_OBSOLETE(M, Bool, database_replicated_ddl_output, true) \
    MAKE_OBSOLETE(M, UInt64, replication_alter_columns_timeout, 60) \
    MAKE_OBSOLETE(M, UInt64, odbc_max_field_size, 0) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_map_type, true) \
    MAKE_OBSOLETE(M, UInt64, merge_tree_clear_old_temporary_directories_interval_seconds, 60) \
    MAKE_OBSOLETE(M, UInt64, merge_tree_clear_old_parts_interval_seconds, 1) \
    MAKE_OBSOLETE(M, UInt64, partial_merge_join_optimizations, 0) \
    MAKE_OBSOLETE(M, MaxThreads, max_alter_threads, 0) \
    /* moved to config.xml: see also src/Core/ServerSettings.h */ \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_buffer_flush_schedule_pool_size, 16) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_pool_size, 16) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, Float, background_merges_mutations_concurrency_ratio, 2) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_move_pool_size, 8) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_fetches_pool_size, 8) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_common_pool_size, 8) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_schedule_pool_size, 128) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_message_broker_schedule_pool_size, 16) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, background_distributed_schedule_pool_size, 16) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, max_remote_read_network_bandwidth_for_server, 0) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, max_remote_write_network_bandwidth_for_server, 0) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, async_insert_threads, 16) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, max_replicated_fetches_network_bandwidth_for_server, 0) \
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, max_replicated_sends_network_bandwidth_for_server, 0) \
    /* ---- */ \
    MAKE_OBSOLETE(M, DefaultDatabaseEngine, default_database_engine, DefaultDatabaseEngine::Atomic) \
    MAKE_OBSOLETE(M, UInt64, max_pipeline_depth, 0)                                                                                 \
    MAKE_OBSOLETE(M, Seconds, temporary_live_view_timeout, 1) \
    MAKE_OBSOLETE(M, Milliseconds, async_insert_cleanup_timeout_ms, 1000) \
    MAKE_OBSOLETE(M, Bool, optimize_fuse_sum_count_avg, 0) \
    MAKE_OBSOLETE(M, Seconds, drain_timeout, 3) \
    MAKE_OBSOLETE(M, UInt64, backup_threads, 16) \
    MAKE_OBSOLETE(M, UInt64, restore_threads, 16) \
    MAKE_OBSOLETE(M, Bool, input_format_arrow_import_nested, false) \
    MAKE_OBSOLETE(M, Bool, input_format_parquet_import_nested, false) \
    MAKE_OBSOLETE(M, Bool, input_format_orc_import_nested, false) \
    MAKE_OBSOLETE(M, Bool, optimize_duplicate_order_by_and_distinct, false) \

    /** The section above is for obsolete settings. Do not add anything there. */


#define FORMAT_FACTORY_SETTINGS(M, ALIAS) \
    M(Char, format_csv_delimiter, ',', "The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.", 0) \
    M(Bool, format_csv_allow_single_quotes, false, "If it is set to true, allow strings in single quotes.", 0) \
    M(Bool, format_csv_allow_double_quotes, true, "If it is set to true, allow strings in double quotes.", 0) \
    M(Bool, output_format_csv_crlf_end_of_line, false, "If it is set true, end of line in CSV format will be \\r\\n instead of \\n.", 0) \
    M(Bool, input_format_csv_enum_as_number, false, "Treat inserted enum values in CSV formats as enum indices", 0) \
    M(Bool, input_format_csv_arrays_as_nested_csv, false, R"(When reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Example: "[""Hello"", ""world"", ""42"""" TV""]". Braces around array can be omitted.)", 0) \
    M(Bool, input_format_skip_unknown_fields, true, "Skip columns with unknown names from input data (it works for JSONEachRow, -WithNames, -WithNamesAndTypes and TSKV formats).", 0) \
    M(Bool, input_format_with_names_use_header, true, "For -WithNames input formats this controls whether format parser is to assume that column data appear in the input exactly as they are specified in the header.", 0) \
    M(Bool, input_format_with_types_use_header, true, "For -WithNamesAndTypes input formats this controls whether format parser should check if data types from the input match data types from the header.", 0) \
    M(Bool, input_format_import_nested_json, false, "Map nested JSON data to nested tables (it works for JSONEachRow format).", 0) \
    M(Bool, input_format_defaults_for_omitted_fields, true, "For input data calculate default expressions for omitted fields (it works for JSONEachRow, -WithNames, -WithNamesAndTypes formats).", IMPORTANT) \
    M(Bool, input_format_csv_empty_as_default, true, "Treat empty fields in CSV input as default values.", 0) \
    M(Bool, input_format_tsv_empty_as_default, false, "Treat empty fields in TSV input as default values.", 0) \
    M(Bool, input_format_tsv_enum_as_number, false, "Treat inserted enum values in TSV formats as enum indices.", 0) \
    M(Bool, input_format_null_as_default, true, "Initialize null fields with default values if the data type of this field is not nullable and it is supported by the input format", 0) \
    M(Bool, input_format_arrow_case_insensitive_column_matching, false, "Ignore case when matching Arrow columns with CH columns.", 0) \
    M(Int64, input_format_orc_row_batch_size, 100'000, "Batch size when reading ORC stripes.", 0) \
    M(Bool, input_format_orc_case_insensitive_column_matching, false, "Ignore case when matching ORC columns with CH columns.", 0) \
    M(Bool, input_format_parquet_case_insensitive_column_matching, false, "Ignore case when matching Parquet columns with CH columns.", 0) \
    M(Bool, input_format_parquet_preserve_order, false, "Avoid reordering rows when reading from Parquet files. Usually makes it much slower.", 0) \
    M(Bool, input_format_parquet_filter_push_down, true, "When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.", 0) \
    M(Bool, input_format_allow_seeks, true, "Allow seeks while reading in ORC/Parquet/Arrow input formats", 0) \
    M(Bool, input_format_orc_allow_missing_columns, false, "Allow missing columns while reading ORC input formats", 0) \
    M(Bool, input_format_orc_use_fast_decoder, true, "Use a faster ORC decoder implementation.", 0) \
    M(Bool, input_format_parquet_allow_missing_columns, false, "Allow missing columns while reading Parquet input formats", 0) \
    M(UInt64, input_format_parquet_local_file_min_bytes_for_seek, 8192, "Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format", 0) \
    M(Bool, input_format_arrow_allow_missing_columns, false, "Allow missing columns while reading Arrow input formats", 0) \
    M(Char, input_format_hive_text_fields_delimiter, '\x01', "Delimiter between fields in Hive Text File", 0) \
    M(Char, input_format_hive_text_collection_items_delimiter, '\x02', "Delimiter between collection(array or map) items in Hive Text File", 0) \
    M(Char, input_format_hive_text_map_keys_delimiter, '\x03', "Delimiter between a pair of map key/values in Hive Text File", 0) \
    M(UInt64, input_format_msgpack_number_of_columns, 0, "The number of columns in inserted MsgPack data. Used for automatic schema inference from data.", 0) \
    M(MsgPackUUIDRepresentation, output_format_msgpack_uuid_representation, FormatSettings::MsgPackUUIDRepresentation::EXT, "The way how to output UUID in MsgPack format.", 0) \
    M(UInt64, input_format_max_rows_to_read_for_schema_inference, 25000, "The maximum rows of data to read for automatic schema inference", 0) \
    M(UInt64, input_format_max_bytes_to_read_for_schema_inference, 32 * 1024 * 1024, "The maximum bytes of data to read for automatic schema inference", 0) \
    M(Bool, input_format_csv_use_best_effort_in_schema_inference, true, "Use some tweaks and heuristics to infer schema in CSV format", 0) \
    M(Bool, input_format_tsv_use_best_effort_in_schema_inference, true, "Use some tweaks and heuristics to infer schema in TSV format", 0) \
    M(Bool, input_format_csv_detect_header, true, "Automatically detect header with names and types in CSV format", 0) \
    M(Bool, input_format_csv_allow_whitespace_or_tab_as_delimiter, false, "Allow to use spaces and tabs(\\t) as field delimiter in the CSV strings", 0) \
    M(Bool, input_format_csv_trim_whitespaces, true, "Trims spaces and tabs (\\t) characters at the beginning and end in CSV strings", 0) \
    M(Bool, input_format_csv_use_default_on_bad_values, false, "Allow to set default value to column when CSV field deserialization failed on bad value", 0) \
    M(Bool, input_format_csv_allow_variable_number_of_columns, false, "Ignore extra columns in CSV input (if file has more columns than expected) and treat missing fields in CSV input as default values", 0) \
    M(Bool, input_format_tsv_allow_variable_number_of_columns, false, "Ignore extra columns in TSV input (if file has more columns than expected) and treat missing fields in TSV input as default values", 0) \
    M(Bool, input_format_custom_allow_variable_number_of_columns, false, "Ignore extra columns in CustomSeparated input (if file has more columns than expected) and treat missing fields in CustomSeparated input as default values", 0) \
    M(Bool, input_format_json_compact_allow_variable_number_of_columns, false, "Ignore extra columns in JSONCompact(EachRow) input (if file has more columns than expected) and treat missing fields in JSONCompact(EachRow) input as default values", 0) \
    M(Bool, input_format_tsv_detect_header, true, "Automatically detect header with names and types in TSV format", 0) \
    M(Bool, input_format_custom_detect_header, true, "Automatically detect header with names and types in CustomSeparated format", 0) \
    M(Bool, input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format Parquet", 0) \
    M(UInt64, input_format_parquet_max_block_size, 8192, "Max block size for parquet reader.", 0) \
    M(Bool, input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference, false, "Skip fields with unsupported types while schema inference for format Protobuf", 0) \
    M(Bool, input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format CapnProto", 0) \
    M(Bool, input_format_orc_skip_columns_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format ORC", 0) \
    M(Bool, input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format Arrow", 0) \
    M(String, column_names_for_schema_inference, "", "The list of column names to use in schema inference for formats without column names. The format: 'column1,column2,column3,...'", 0) \
    M(String, schema_inference_hints, "", "The list of column names and types to use in schema inference for formats without column names. The format: 'column_name1 column_type1, column_name2 column_type2, ...'", 0) \
    M(Bool, schema_inference_make_columns_nullable, true, "If set to true, all inferred types will be Nullable in schema inference for formats without information about nullability.", 0) \
    M(Bool, input_format_json_read_bools_as_numbers, true, "Allow to parse bools as numbers in JSON input formats", 0) \
    M(Bool, input_format_json_try_infer_numbers_from_strings, true, "Try to infer numbers from string fields while schema inference", 0) \
    M(Bool, input_format_json_validate_types_from_metadata, true, "For JSON/JSONCompact/JSONColumnsWithMetadata input formats this controls whether format parser should check if data types from input metadata match data types of the corresponding columns from the table", 0) \
    M(Bool, input_format_json_read_numbers_as_strings, false, "Allow to parse numbers as strings in JSON input formats", 0) \
    M(Bool, input_format_json_read_objects_as_strings, true, "Allow to parse JSON objects as strings in JSON input formats", 0) \
    M(Bool, input_format_json_named_tuples_as_objects, true, "Deserialize named tuple columns as JSON objects", 0) \
    M(Bool, input_format_json_ignore_unknown_keys_in_named_tuple, true, "Ignore unknown keys in json object for named tuples", 0) \
    M(Bool, input_format_json_defaults_for_missing_elements_in_named_tuple, true, "Insert default value in named tuple element if it's missing in json object", 0) \
    M(Bool, input_format_try_infer_integers, true, "Try to infer integers instead of floats while schema inference in text formats", 0) \
    M(Bool, input_format_try_infer_dates, true, "Try to infer dates from string fields while schema inference in text formats", 0) \
    M(Bool, input_format_try_infer_datetimes, true, "Try to infer datetimes from string fields while schema inference in text formats", 0) \
    M(Bool, input_format_protobuf_flatten_google_wrappers, false, "Enable Google wrappers for regular non-nested columns, e.g. google.protobuf.StringValue 'str' for String column 'str'. For Nullable columns empty wrappers are recognized as defaults, and missing as nulls", 0) \
    M(Bool, output_format_protobuf_nullables_with_google_wrappers, false, "When serializing Nullable columns with Google wrappers, serialize default values as empty wrappers. If turned off, default and null values are not serialized", 0) \
    M(UInt64, input_format_csv_skip_first_lines, 0, "Skip specified number of lines at the beginning of data in CSV format", 0) \
    M(UInt64, input_format_tsv_skip_first_lines, 0, "Skip specified number of lines at the beginning of data in TSV format", 0) \
    M(Bool, input_format_csv_skip_trailing_empty_lines, false, "Skip trailing empty lines in CSV format", 0) \
    M(Bool, input_format_tsv_skip_trailing_empty_lines, false, "Skip trailing empty lines in TSV format", 0) \
    M(Bool, input_format_custom_skip_trailing_empty_lines, false, "Skip trailing empty lines in CustomSeparated format", 0) \
    \
    M(Bool, input_format_native_allow_types_conversion, true, "Allow data types conversion in Native input format", 0) \
    \
    M(DateTimeInputFormat, date_time_input_format, FormatSettings::DateTimeInputFormat::Basic, "Method to read DateTime from text input formats. Possible values: 'basic', 'best_effort' and 'best_effort_us'.", 0) \
    M(DateTimeOutputFormat, date_time_output_format, FormatSettings::DateTimeOutputFormat::Simple, "Method to write DateTime to text output. Possible values: 'simple', 'iso', 'unix_timestamp'.", 0) \
    M(IntervalOutputFormat, interval_output_format, FormatSettings::IntervalOutputFormat::Numeric, "Textual representation of Interval. Possible values: 'kusto', 'numeric'.", 0) \
    \
    M(Bool, input_format_ipv4_default_on_conversion_error, false, "Deserialization of IPv4 will use default values instead of throwing exception on conversion error.", 0) \
    M(Bool, input_format_ipv6_default_on_conversion_error, false, "Deserialization of IPV6 will use default values instead of throwing exception on conversion error.", 0) \
    M(String, bool_true_representation, "true", "Text to represent bool value in TSV/CSV formats.", 0) \
    M(String, bool_false_representation, "false", "Text to represent bool value in TSV/CSV formats.", 0) \
    \
    M(Bool, input_format_values_interpret_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.", 0) \
    M(Bool, input_format_values_deduce_templates_of_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.", 0) \
    M(Bool, input_format_values_accurate_types_of_literals, true, "For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.", 0) \
    M(Bool, input_format_avro_allow_missing_fields, false, "For Avro/AvroConfluent format: when field is not found in schema use default value instead of error", 0) \
    /** This setting is obsolete and do nothing, left for compatibility reasons. */ \
    M(Bool, input_format_avro_null_as_default, false, "For Avro/AvroConfluent format: insert default in case of null and non Nullable column", 0) \
    M(UInt64, format_binary_max_string_size, 1_GiB, "The maximum allowed size for String in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit", 0) \
    M(UInt64, format_binary_max_array_size, 1_GiB, "The maximum allowed size for Array in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit", 0) \
    M(URI, format_avro_schema_registry_url, "", "For AvroConfluent format: Confluent Schema Registry URL.", 0) \
    \
    M(Bool, output_format_json_quote_64bit_integers, true, "Controls quoting of 64-bit integers in JSON output format.", 0) \
    M(Bool, output_format_json_quote_denormals, false, "Enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.", 0) \
    M(Bool, output_format_json_quote_decimals, false, "Controls quoting of decimals in JSON output format.", 0) \
    M(Bool, output_format_json_quote_64bit_floats, false, "Controls quoting of 64-bit float numbers in JSON output format.", 0) \
    \
    M(Bool, output_format_json_escape_forward_slashes, true, "Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don't confuse with backslashes that are always escaped.", 0) \
    M(Bool, output_format_json_named_tuples_as_objects, true, "Serialize named tuple columns as JSON objects.", 0) \
    M(Bool, output_format_json_array_of_rows, false, "Output a JSON array of all rows in JSONEachRow(Compact) format.", 0) \
    M(Bool, output_format_json_validate_utf8, false, "Validate UTF-8 sequences in JSON output formats, doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate utf8", 0) \
    \
    M(String, format_json_object_each_row_column_for_object_name, "", "The name of column that will be used as object names in JSONObjectEachRow format. Column type should be String", 0) \
    \
    M(UInt64, output_format_pretty_max_rows, 10000, "Rows limit for Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_column_pad_width, 250, "Maximum width to pad all values in a column in Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_value_width, 10000, "Maximum width of value to display in Pretty formats. If greater - it will be cut.", 0) \
    M(Bool, output_format_pretty_color, true, "Use ANSI escape sequences to paint colors in Pretty formats", 0) \
    M(String, output_format_pretty_grid_charset, "UTF-8", "Charset for printing grid borders. Available charsets: ASCII, UTF-8 (default one).", 0) \
    M(UInt64, output_format_parquet_row_group_size, 1000000, "Target row group size in rows.", 0) \
    M(UInt64, output_format_parquet_row_group_size_bytes, 512 * 1024 * 1024, "Target row group size in bytes, before compression.", 0) \
    M(Bool, output_format_parquet_string_as_string, false, "Use Parquet String type instead of Binary for String columns.", 0) \
    M(Bool, output_format_parquet_fixed_string_as_fixed_byte_array, true, "Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary for FixedString columns.", 0) \
    M(ParquetVersion, output_format_parquet_version, "2.latest", "Parquet format version for output format. Supported versions: 1.0, 2.4, 2.6 and 2.latest (default)", 0) \
    M(ParquetCompression, output_format_parquet_compression_method, "lz4", "Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)", 0) \
    M(Bool, output_format_parquet_compliant_nested_types, true, "In parquet file schema, use name 'element' instead of 'item' for list elements. This is a historical artifact of Arrow library implementation. Generally increases compatibility, except perhaps with some old versions of Arrow.", 0) \
    M(Bool, output_format_parquet_use_custom_encoder, false, "Use a faster Parquet encoder implementation.", 0) \
    M(Bool, output_format_parquet_parallel_encoding, true, "Do Parquet encoding in multiple threads. Requires output_format_parquet_use_custom_encoder.", 0) \
    M(UInt64, output_format_parquet_data_page_size, 1024 * 1024, "Target page size in bytes, before compression.", 0) \
    M(UInt64, output_format_parquet_batch_size, 1024, "Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.", 0) \
    M(String, output_format_avro_codec, "", "Compression codec used for output. Possible values: 'null', 'deflate', 'snappy'.", 0) \
    M(UInt64, output_format_avro_sync_interval, 16 * 1024, "Sync interval in bytes.", 0) \
    M(String, output_format_avro_string_column_pattern, "", "For Avro format: regexp of String columns to select as AVRO string.", 0) \
    M(UInt64, output_format_avro_rows_in_file, 1, "Max rows in a file (if permitted by storage)", 0) \
    M(Bool, output_format_tsv_crlf_end_of_line, false, "If it is set true, end of line in TSV format will be \\r\\n instead of \\n.", 0) \
    M(String, format_csv_null_representation, "\\N", "Custom NULL representation in CSV format", 0) \
    M(String, format_tsv_null_representation, "\\N", "Custom NULL representation in TSV format", 0) \
    M(Bool, output_format_decimal_trailing_zeros, false, "Output trailing zeros when printing Decimal values. E.g. 1.230000 instead of 1.23.", 0) \
    \
    M(UInt64, input_format_allow_errors_num, 0, "Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    M(Float, input_format_allow_errors_ratio, 0, "Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    M(String, input_format_record_errors_file_path, "", "Path of the file used to record errors while reading text formats (CSV, TSV).", 0) \
    M(String, errors_output_format, "CSV", "Method to write Errors to text output.", 0) \
    \
    M(String, format_schema, "", "Schema identifier (used by schema-based formats)", 0) \
    M(String, format_template_resultset, "", "Path to file which contains format string for result set (for Template format)", 0) \
    M(String, format_template_row, "", "Path to file which contains format string for rows (for Template format)", 0) \
    M(String, format_template_rows_between_delimiter, "\n", "Delimiter between rows (for Template format)", 0) \
    \
    M(EscapingRule, format_custom_escaping_rule, "Escaped", "Field escaping rule (for CustomSeparated format)", 0) \
    M(String, format_custom_field_delimiter, "\t", "Delimiter between fields (for CustomSeparated format)", 0) \
    M(String, format_custom_row_before_delimiter, "", "Delimiter before field of the first column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_after_delimiter, "\n", "Delimiter after field of the last column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_between_delimiter, "", "Delimiter between rows (for CustomSeparated format)", 0) \
    M(String, format_custom_result_before_delimiter, "", "Prefix before result set (for CustomSeparated format)", 0) \
    M(String, format_custom_result_after_delimiter, "", "Suffix after result set (for CustomSeparated format)", 0) \
    \
    M(String, format_regexp, "", "Regular expression (for Regexp format)", 0) \
    M(EscapingRule, format_regexp_escaping_rule, "Raw", "Field escaping rule (for Regexp format)", 0) \
    M(Bool, format_regexp_skip_unmatched, false, "Skip lines unmatched by regular expression (for Regexp format)", 0) \
    \
    M(Bool, output_format_enable_streaming, false, "Enable streaming in output formats that support it.", 0) \
    M(Bool, output_format_write_statistics, true, "Write statistics about read rows, bytes, time elapsed in suitable output formats.", 0) \
    M(Bool, output_format_pretty_row_numbers, false, "Add row numbers before each row for pretty output format", 0) \
    M(Bool, insert_distributed_one_random_shard, false, "If setting is enabled, inserting into distributed table will choose a random shard to write when there is no sharding key", 0) \
    \
    M(Bool, exact_rows_before_limit, false, "When enabled, ClickHouse will provide exact value for rows_before_limit_at_least statistic, but with the cost that the data before limit will have to be read completely", 0) \
    M(UInt64, cross_to_inner_join_rewrite, 1, "Use inner join instead of comma/cross join if there're joining expressions in the WHERE section. Values: 0 - no rewrite, 1 - apply if possible for comma/cross, 2 - force rewrite all comma joins, cross - if possible", 0) \
    \
    M(Bool, output_format_arrow_low_cardinality_as_dictionary, false, "Enable output LowCardinality type as Dictionary Arrow type", 0) \
    M(Bool, output_format_arrow_string_as_string, false, "Use Arrow String type instead of Binary for String columns", 0) \
    M(Bool, output_format_arrow_fixed_string_as_fixed_byte_array, true, "Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.", 0) \
    M(ArrowCompression, output_format_arrow_compression_method, "lz4_frame", "Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)", 0) \
    \
    M(Bool, output_format_orc_string_as_string, false, "Use ORC String type instead of Binary for String columns", 0) \
    M(ORCCompression, output_format_orc_compression_method, "lz4", "Compression method for ORC output format. Supported codecs: lz4, snappy, zlib, zstd, none (uncompressed)", 0) \
    \
    M(CapnProtoEnumComparingMode, format_capn_proto_enum_comparising_mode, FormatSettings::CapnProtoEnumComparingMode::BY_VALUES, "How to map ClickHouse Enum and CapnProto Enum", 0) \
    \
    M(Bool, format_capn_proto_use_autogenerated_schema, true, "Use autogenerated CapnProto schema when format_schema is not set", 0) \
    M(Bool, format_protobuf_use_autogenerated_schema, true, "Use autogenerated Protobuf when format_schema is not set", 0) \
    M(String, output_format_schema, "", "The path to the file where the automatically generated schema will be saved", 0) \
    \
    M(String, input_format_mysql_dump_table_name, "", "Name of the table in MySQL dump from which to read data", 0) \
    M(Bool, input_format_mysql_dump_map_column_names, true, "Match columns from table in MySQL dump and columns from ClickHouse table by names", 0) \
    \
    M(UInt64, output_format_sql_insert_max_batch_size, DEFAULT_BLOCK_SIZE, "The maximum number  of rows in one INSERT statement.", 0) \
    M(String, output_format_sql_insert_table_name, "table", "The name of table in the output INSERT query", 0) \
    M(Bool, output_format_sql_insert_include_column_names, true, "Include column names in INSERT query", 0) \
    M(Bool, output_format_sql_insert_use_replace, false, "Use REPLACE statement instead of INSERT", 0) \
    M(Bool, output_format_sql_insert_quote_names, true, "Quote column names with '`' characters", 0) \
    \
    M(Bool, output_format_bson_string_as_string, false, "Use BSON String type instead of Binary for String columns.", 0) \
    M(Bool, input_format_bson_skip_fields_with_unsupported_types_in_schema_inference, false, "Skip fields with unsupported types while schema inference for format BSON.", 0) \
    \
    M(Bool, format_display_secrets_in_show_and_select, false, "Do not hide secrets in SHOW and SELECT queries.", IMPORTANT) \
    M(Bool, regexp_dict_allow_hyperscan, true, "Allow regexp_tree dictionary using Hyperscan library.", 0) \
    \
    M(Bool, dictionary_use_async_executor, false, "Execute a pipeline for reading from a dictionary with several threads. It's supported only by DIRECT dictionary with CLICKHOUSE source.", 0) \
    M(Bool, precise_float_parsing, false, "Prefer more precise (but slower) float parsing algorithm", 0) \

// End of FORMAT_FACTORY_SETTINGS
// Please add settings non-related to formats into the COMMON_SETTINGS above.

#define LIST_OF_SETTINGS(M, ALIAS)    \
    COMMON_SETTINGS(M, ALIAS)         \
    OBSOLETE_SETTINGS(M, ALIAS)       \
    FORMAT_FACTORY_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(SettingsTraits, LIST_OF_SETTINGS)


/** Settings of query execution.
  * These settings go to users.xml.
  */
struct Settings : public BaseSettings<SettingsTraits>, public IHints<2, Settings>
{
    Settings() = default;

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
     * The profile can also be set using the `set` functions, like the profile setting.
     */
    void setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config);

    /// Load settings from configuration file, at "path" prefix in configuration.
    void loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

    /// Dumps profile events to column of type Map(String, String)
    void dumpToMapColumn(IColumn * column, bool changed_only = true);

    /// Check that there is no user-level settings at the top level in config.
    /// This is a common source of mistake (user don't know where to write user-level setting).
    static void checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path);

    std::vector<String> getAllRegisteredNames() const override;

    void set(std::string_view name, const Field & value) override;

    void setDefaultValue(const String & name) { resetToDefault(name); }

private:
    void applyCompatibilitySetting(const String & compatibility);

    std::unordered_set<std::string_view> settings_changed_by_compatibility_setting;
};

/*
 * User-specified file format settings for File and URL engines.
 */
DECLARE_SETTINGS_TRAITS(FormatFactorySettingsTraits, FORMAT_FACTORY_SETTINGS)

struct FormatFactorySettings : public BaseSettings<FormatFactorySettingsTraits>
{
};

}
