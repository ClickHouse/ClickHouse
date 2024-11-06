#include <Columns/ColumnMap.h>
#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/BaseSettingsProgramOptions.h>
#include <Core/DistributedCacheProtocol.h>
#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>
#include <Core/SettingsChangesHistory.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <IO/ReadBufferFromString.h>
#include <IO/S3Defines.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <base/types.h>
#include <Common/NamePrompter.h>
#include <Common/typeid_cast.h>

#include <boost/program_options.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
}

/** List of settings: type, name, default value, description, flags
  *
  * This looks rather inconvenient. It is done that way to avoid repeating settings in different places.
  * Note: as an alternative, we could implement settings to be completely dynamic in the form of the map: String -> Field,
  *  but we are not going to do it, because settings are used everywhere as static struct fields.
  *
  * `flags` can include a Tier (BETA | EXPERIMENTAL) and an optional bitwise AND with IMPORTANT.
  * The default (0) means a PRODUCTION ready setting
  *
  * A setting is "IMPORTANT" if it affects the results of queries and can't be ignored by older versions.
  * Tiers:
  * EXPERIMENTAL: The feature is in active development stage. Mostly for developers or for ClickHouse enthusiasts.
  * BETA: There are no known bugs problems in the functionality, but the outcome of using it together with other
  * features/components is unknown and correctness is not guaranteed.
  * PRODUCTION (Default): The feature is safe to use along with other features from the PRODUCTION tier.
  *
  * When adding new or changing existing settings add them to the settings changes history in SettingsChangesHistory.cpp
  * for tracking settings changes in different versions and for special `compatibility` settings to work correctly.
  */

// clang-format off
#if defined(__CLION_IDE__)
/// CLion freezes for a minute every time it processes this
#define COMMON_SETTINGS(DECLARE, ALIAS)
#define OBSOLETE_SETTINGS(DECLARE, ALIAS)
#else
#define COMMON_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Dialect, dialect, Dialect::clickhouse, R"(
Which dialect will be used to parse query
)", 0)\
    DECLARE(UInt64, min_compress_block_size, 65536, R"(
For [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least `min_compress_block_size`. By default, 65,536.

The actual size of the block, if the uncompressed data is less than `max_compress_block_size`, is no less than this value and no less than the volume of data for one mark.

Let’s look at an example. Assume that `index_granularity` was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min_compress_block_size = 65,536, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won’t be decompressed.

:::note
This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse.
:::
)", 0) \
    DECLARE(UInt64, max_compress_block_size, 1048576, R"(
The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). Specifying a smaller block size generally leads to slightly reduced compression ratio, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced.

:::note
This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse.
:::

Don’t confuse blocks for compression (a chunk of memory consisting of bytes) with blocks for query processing (a set of rows from a table).
)", 0) \
    DECLARE(UInt64, max_block_size, DEFAULT_BLOCK_SIZE, R"(
In ClickHouse, data is processed by blocks, which are sets of column parts. The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block.

The `max_block_size` setting indicates the recommended maximum number of rows to include in a single block when loading data from tables. Blocks the size of `max_block_size` are not always loaded from the table: if ClickHouse determines that less data needs to be retrieved, a smaller block is processed.

The block size should not be too small to avoid noticeable costs when processing each block. It should also not be too large to ensure that queries with a LIMIT clause execute quickly after processing the first block. When setting `max_block_size`, the goal should be to avoid consuming too much memory when extracting a large number of columns in multiple threads and to preserve at least some cache locality.
)", 0) \
    DECLARE(UInt64, max_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE, R"(
The size of blocks (in a count of rows) to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the ‘max_insert_block_size’ setting on the server does not affect the size of the inserted blocks.
The setting also does not have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

The default is slightly more than `max_block_size`. The reason for this is that certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allow sorting more data in RAM.
)", 0) \
    DECLARE(UInt64, min_insert_block_size_rows, DEFAULT_INSERT_BLOCK_SIZE, R"(
Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
)", 0) \
    DECLARE(UInt64, min_insert_block_size_bytes, (DEFAULT_INSERT_BLOCK_SIZE * 256), R"(
Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
)", 0) \
    DECLARE(UInt64, min_insert_block_size_rows_for_materialized_views, 0, R"(
Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

**See Also**

- [min_insert_block_size_rows](#min-insert-block-size-rows)
)", 0) \
    DECLARE(UInt64, min_insert_block_size_bytes_for_materialized_views, 0, R"(
Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

**See also**

- [min_insert_block_size_bytes](#min-insert-block-size-bytes)
)", 0) \
    DECLARE(UInt64, min_external_table_block_size_rows, DEFAULT_INSERT_BLOCK_SIZE, R"(
Squash blocks passed to external table to specified size in rows, if blocks are not big enough.
)", 0) \
    DECLARE(UInt64, min_external_table_block_size_bytes, (DEFAULT_INSERT_BLOCK_SIZE * 256), R"(
Squash blocks passed to the external table to a specified size in bytes, if blocks are not big enough.
)", 0) \
    DECLARE(UInt64, max_joined_block_size_rows, DEFAULT_BLOCK_SIZE, R"(
Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.
)", 0) \
    DECLARE(UInt64, max_insert_threads, 0, R"(
The maximum number of threads to execute the `INSERT SELECT` query.

Possible values:

- 0 (or 1) — `INSERT SELECT` no parallel execution.
- Positive integer. Bigger than 1.

Cloud default value: from `2` to `4`, depending on the service size.

Parallel `INSERT SELECT` has effect only if the `SELECT` part is executed in parallel, see [max_threads](#max_threads) setting.
Higher values will lead to higher memory usage.
)", 0) \
    DECLARE(UInt64, max_insert_delayed_streams_for_parallel_write, 0, R"(
The maximum number of streams (columns) to delay final part flush. Default - auto (1000 in case of underlying storage supports parallel write, for example S3 and disabled otherwise)
)", 0) \
    DECLARE(MaxThreads, max_final_threads, 0, R"(
Sets the maximum number of parallel threads for the `SELECT` query data read phase with the [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier.

Possible values:

- Positive integer.
- 0 or 1 — Disabled. `SELECT` queries are executed in a single thread.
)", 0) \
    DECLARE(UInt64, max_threads_for_indexes, 0, R"(
The maximum number of threads process indices.
)", 0) \
    DECLARE(MaxThreads, max_threads, 0, R"(
The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max_distributed_connections’ parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, when reading from a table, if it is possible to evaluate expressions with functions, filter with WHERE and pre-aggregate for GROUP BY in parallel using at least ‘max_threads’ number of threads, then ‘max_threads’ are used.

For queries that are completed quickly because of a LIMIT, you can set a lower ‘max_threads’. For example, if the necessary number of entries are located in every block and max_threads = 8, then 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.
)", 0) \
    DECLARE(Bool, use_concurrency_control, true, R"(
Respect the server's concurrency control (see the `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores` global server settings). If disabled, it allows using a larger number of threads even if the server is overloaded (not recommended for normal usage, and needed mostly for tests).
)", 0) \
    DECLARE(MaxThreads, max_download_threads, 4, R"(
The maximum number of threads to download data (e.g. for URL engine).
)", 0) \
    DECLARE(MaxThreads, max_parsing_threads, 0, R"(
The maximum number of threads to parse data in input formats that support parallel parsing. By default, it is determined automatically
)", 0) \
    DECLARE(UInt64, max_download_buffer_size, 10*1024*1024, R"(
The maximal size of buffer for parallel downloading (e.g. for URL engine) per each thread.
)", 0) \
    DECLARE(UInt64, max_read_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, R"(
The maximum size of the buffer to read from the filesystem.
)", 0) \
    DECLARE(UInt64, max_read_buffer_size_local_fs, 128*1024, R"(
The maximum size of the buffer to read from local filesystem. If set to 0 then max_read_buffer_size will be used.
)", 0) \
    DECLARE(UInt64, max_read_buffer_size_remote_fs, 0, R"(
The maximum size of the buffer to read from remote filesystem. If set to 0 then max_read_buffer_size will be used.
)", 0) \
    DECLARE(UInt64, max_distributed_connections, 1024, R"(
The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.
)", 0) \
    DECLARE(UInt64, max_query_size, DBMS_DEFAULT_MAX_QUERY_SIZE, R"(
The maximum number of bytes of a query string parsed by the SQL parser.
Data in the VALUES clause of INSERT queries is processed by a separate stream parser (that consumes O(1) RAM) and not affected by this restriction.

:::note
`max_query_size` cannot be set within an SQL query (e.g., `SELECT now() SETTINGS max_query_size=10000`) because ClickHouse needs to allocate a buffer to parse the query, and this buffer size is determined by the `max_query_size` setting, which must be configured before the query is executed.
:::
)", 0) \
    DECLARE(UInt64, interactive_delay, 100000, R"(
The interval in microseconds for checking whether request execution has been canceled and sending the progress.
)", 0) \
    DECLARE(Seconds, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, R"(
Connection timeout if there are no replicas.
)", 0) \
    DECLARE(Milliseconds, handshake_timeout_ms, 10000, R"(
Timeout in milliseconds for receiving Hello packet from replicas during handshake.
)", 0) \
    DECLARE(Milliseconds, connect_timeout_with_failover_ms, 1000, R"(
The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the ‘shard’ and ‘replica’ sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.
)", 0) \
    DECLARE(Milliseconds, connect_timeout_with_failover_secure_ms, 1000, R"(
Connection timeout for selecting first healthy replica (for secure connections).
)", 0) \
    DECLARE(Seconds, receive_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, R"(
Timeout for receiving data from the network, in seconds. If no bytes were received in this interval, the exception is thrown. If you set this setting on the client, the 'send_timeout' for the socket will also be set on the corresponding connection end on the server.
)", 0) \
    DECLARE(Seconds, send_timeout, DBMS_DEFAULT_SEND_TIMEOUT_SEC, R"(
Timeout for sending data to the network, in seconds. If a client needs to send some data but is not able to send any bytes in this interval, the exception is thrown. If you set this setting on the client, the 'receive_timeout' for the socket will also be set on the corresponding connection end on the server.
)", 0) \
    DECLARE(Seconds, tcp_keep_alive_timeout, DEFAULT_TCP_KEEP_ALIVE_TIMEOUT /* less than DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC */, R"(
The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes
)", 0) \
    DECLARE(Milliseconds, hedged_connection_timeout_ms, 50, R"(
Connection timeout for establishing connection with replica for Hedged requests
)", 0) \
    DECLARE(Milliseconds, receive_data_timeout_ms, 2000, R"(
Connection timeout for receiving first packet of data or packet with positive progress from replica
)", 0) \
    DECLARE(Bool, use_hedged_requests, true, R"(
Enables hedged requests logic for remote queries. It allows to establish many connections with different replicas for query.
New connection is enabled in case existent connection(s) with replica(s) were not established within `hedged_connection_timeout`
or no data was received within `receive_data_timeout`. Query uses the first connection which send non empty progress packet (or data packet, if `allow_changing_replica_until_first_data_packet`);
other connections are cancelled. Queries with `max_parallel_replicas > 1` are supported.

Enabled by default.

Disabled by default on Cloud.
)", 0) \
    DECLARE(Bool, allow_changing_replica_until_first_data_packet, false, R"(
If it's enabled, in hedged requests we can start new connection until receiving first data packet even if we have already made some progress
(but progress haven't updated for `receive_data_timeout` timeout), otherwise we disable changing replica after the first time we made progress.
)", 0) \
    DECLARE(Milliseconds, queue_max_wait_ms, 0, R"(
The wait time in the request queue, if the number of concurrent requests exceeds the maximum.
)", 0) \
    DECLARE(Milliseconds, connection_pool_max_wait_ms, 0, R"(
The wait time in milliseconds for a connection when the connection pool is full.

Possible values:

- Positive integer.
- 0 — Infinite timeout.
)", 0) \
    DECLARE(Milliseconds, replace_running_query_max_wait_ms, 5000, R"(
The wait time for running the query with the same `query_id` to finish, when the [replace_running_query](#replace-running-query) setting is active.

Possible values:

- Positive integer.
- 0 — Throwing an exception that does not allow to run a new query if the server already executes a query with the same `query_id`.
)", 0) \
    DECLARE(Milliseconds, kafka_max_wait_ms, 5000, R"(
The wait time in milliseconds for reading messages from [Kafka](../../engines/table-engines/integrations/kafka.md/#kafka) before retry.

Possible values:

- Positive integer.
- 0 — Infinite timeout.

See also:

- [Apache Kafka](https://kafka.apache.org/)
)", 0) \
    DECLARE(Milliseconds, rabbitmq_max_wait_ms, 5000, R"(
The wait time for reading from RabbitMQ before retry.
)", 0) \
    DECLARE(UInt64, poll_interval, DBMS_DEFAULT_POLL_INTERVAL, R"(
Block at the query wait loop on the server for the specified number of seconds.
)", 0) \
    DECLARE(UInt64, idle_connection_timeout, 3600, R"(
Timeout to close idle TCP connections after specified number of seconds.

Possible values:

- Positive integer (0 - close immediately, after 0 seconds).
)", 0) \
    DECLARE(UInt64, distributed_connections_pool_size, 1024, R"(
The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.
)", 0) \
    DECLARE(UInt64, connections_with_failover_max_tries, 3, R"(
The maximum number of connection attempts with each replica for the Distributed table engine.
)", 0) \
    DECLARE(UInt64, s3_strict_upload_part_size, S3::DEFAULT_STRICT_UPLOAD_PART_SIZE, R"(
The exact size of part to upload during multipart upload to S3 (some implementations does not supports variable size parts).
)", 0) \
    DECLARE(UInt64, azure_strict_upload_part_size, 0, R"(
The exact size of part to upload during multipart upload to Azure blob storage.
)", 0) \
    DECLARE(UInt64, azure_max_blocks_in_multipart_upload, 50000, R"(
Maximum number of blocks in multipart upload for Azure.
)", 0) \
    DECLARE(UInt64, s3_min_upload_part_size, S3::DEFAULT_MIN_UPLOAD_PART_SIZE, R"(
The minimum size of part to upload during multipart upload to S3.
)", 0) \
    DECLARE(UInt64, s3_max_upload_part_size, S3::DEFAULT_MAX_UPLOAD_PART_SIZE, R"(
The maximum size of part to upload during multipart upload to S3.
)", 0) \
    DECLARE(UInt64, azure_min_upload_part_size, 16*1024*1024, R"(
The minimum size of part to upload during multipart upload to Azure blob storage.
)", 0) \
    DECLARE(UInt64, azure_max_upload_part_size, 5ull*1024*1024*1024, R"(
The maximum size of part to upload during multipart upload to Azure blob storage.
)", 0) \
    DECLARE(UInt64, s3_upload_part_size_multiply_factor, S3::DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_FACTOR, R"(
Multiply s3_min_upload_part_size by this factor each time s3_multiply_parts_count_threshold parts were uploaded from a single write to S3.
)", 0) \
    DECLARE(UInt64, s3_upload_part_size_multiply_parts_count_threshold, S3::DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_PARTS_COUNT_THRESHOLD, R"(
Each time this number of parts was uploaded to S3, s3_min_upload_part_size is multiplied by s3_upload_part_size_multiply_factor.
)", 0) \
    DECLARE(UInt64, s3_max_part_number, S3::DEFAULT_MAX_PART_NUMBER, R"(
Maximum part number number for s3 upload part.
)", 0) \
    DECLARE(UInt64, s3_max_single_operation_copy_size, S3::DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE, R"(
Maximum size for a single copy operation in s3
)", 0) \
    DECLARE(UInt64, azure_upload_part_size_multiply_factor, 2, R"(
Multiply azure_min_upload_part_size by this factor each time azure_multiply_parts_count_threshold parts were uploaded from a single write to Azure blob storage.
)", 0) \
    DECLARE(UInt64, azure_upload_part_size_multiply_parts_count_threshold, 500, R"(
Each time this number of parts was uploaded to Azure blob storage, azure_min_upload_part_size is multiplied by azure_upload_part_size_multiply_factor.
)", 0) \
    DECLARE(UInt64, s3_max_inflight_parts_for_one_file, S3::DEFAULT_MAX_INFLIGHT_PARTS_FOR_ONE_FILE, R"(
The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited.
)", 0) \
    DECLARE(UInt64, azure_max_inflight_parts_for_one_file, 20, R"(
The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited.
)", 0) \
    DECLARE(UInt64, s3_max_single_part_upload_size, S3::DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE, R"(
The maximum size of object to upload using singlepart upload to S3.
)", 0) \
    DECLARE(UInt64, azure_max_single_part_upload_size, 100*1024*1024, R"(
The maximum size of object to upload using singlepart upload to Azure blob storage.
)", 0)                                                                             \
    DECLARE(UInt64, azure_max_single_part_copy_size, 256*1024*1024, R"(
The maximum size of object to copy using single part copy to Azure blob storage.
)", 0) \
    DECLARE(UInt64, s3_max_single_read_retries, S3::DEFAULT_MAX_SINGLE_READ_TRIES, R"(
The maximum number of retries during single S3 read.
)", 0) \
    DECLARE(UInt64, azure_max_single_read_retries, 4, R"(
The maximum number of retries during single Azure blob storage read.
)", 0) \
    DECLARE(UInt64, azure_max_unexpected_write_error_retries, 4, R"(
The maximum number of retries in case of unexpected errors during Azure blob storage write
)", 0) \
    DECLARE(UInt64, s3_max_unexpected_write_error_retries, S3::DEFAULT_MAX_UNEXPECTED_WRITE_ERROR_RETRIES, R"(
The maximum number of retries in case of unexpected errors during S3 write.
)", 0) \
    DECLARE(UInt64, s3_max_redirects, S3::DEFAULT_MAX_REDIRECTS, R"(
Max number of S3 redirects hops allowed.
)", 0) \
    DECLARE(UInt64, s3_max_connections, S3::DEFAULT_MAX_CONNECTIONS, R"(
The maximum number of connections per server.
)", 0) \
    DECLARE(UInt64, s3_max_get_rps, 0, R"(
Limit on S3 GET request per second rate before throttling. Zero means unlimited.
)", 0) \
    DECLARE(UInt64, s3_max_get_burst, 0, R"(
Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_get_rps`
)", 0) \
    DECLARE(UInt64, s3_max_put_rps, 0, R"(
Limit on S3 PUT request per second rate before throttling. Zero means unlimited.
)", 0) \
    DECLARE(UInt64, s3_max_put_burst, 0, R"(
Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_put_rps`
)", 0) \
    DECLARE(UInt64, s3_list_object_keys_size, S3::DEFAULT_LIST_OBJECT_KEYS_SIZE, R"(
Maximum number of files that could be returned in batch by ListObject request
)", 0) \
    DECLARE(Bool, s3_use_adaptive_timeouts, S3::DEFAULT_USE_ADAPTIVE_TIMEOUTS, R"(
When set to `true` than for all s3 requests first two attempts are made with low send and receive timeouts.
When set to `false` than all attempts are made with identical timeouts.
)", 0) \
    DECLARE(UInt64, azure_list_object_keys_size, 1000, R"(
Maximum number of files that could be returned in batch by ListObject request
)", 0) \
    DECLARE(Bool, s3_truncate_on_insert, false, R"(
Enables or disables truncate before inserts in s3 engine tables. If disabled, an exception will be thrown on insert attempts if an S3 object already exists.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.
)", 0) \
    DECLARE(Bool, azure_truncate_on_insert, false, R"(
Enables or disables truncate before insert in azure engine tables.
)", 0) \
    DECLARE(Bool, s3_create_new_file_on_insert, false, R"(
Enables or disables creating a new file on each insert in s3 engine tables. If enabled, on each insert a new S3 object will be created with the key, similar to this pattern:

initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.
)", 0) \
    DECLARE(Bool, s3_skip_empty_files, false, R"(
Enables or disables skipping empty files in [S3](../../engines/table-engines/integrations/s3.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.
)", 0) \
    DECLARE(Bool, azure_create_new_file_on_insert, false, R"(
Enables or disables creating a new file on each insert in azure engine tables
)", 0) \
    DECLARE(Bool, s3_check_objects_after_upload, false, R"(
Check each uploaded object to s3 with head request to be sure that upload was successful
)", 0) \
    DECLARE(Bool, azure_check_objects_after_upload, false, R"(
Check each uploaded object in azure blob storage to be sure that upload was successful
)", 0) \
    DECLARE(Bool, s3_allow_parallel_part_upload, true, R"(
Use multiple threads for s3 multipart upload. It may lead to slightly higher memory usage
)", 0) \
    DECLARE(Bool, azure_allow_parallel_part_upload, true, R"(
Use multiple threads for azure multipart upload.
)", 0) \
    DECLARE(Bool, s3_throw_on_zero_files_match, false, R"(
Throw an error, when ListObjects request cannot match any files
)", 0) \
    DECLARE(Bool, hdfs_throw_on_zero_files_match, false, R"(
Throw an error if matched zero files according to glob expansion rules.

Possible values:
- 1 — `SELECT` throws an exception.
- 0 — `SELECT` returns empty result.
)", 0) \
    DECLARE(Bool, azure_throw_on_zero_files_match, false, R"(
Throw an error if matched zero files according to glob expansion rules.

Possible values:
- 1 — `SELECT` throws an exception.
- 0 — `SELECT` returns empty result.
)", 0) \
    DECLARE(Bool, s3_ignore_file_doesnt_exist, false, R"(
Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.
)", 0) \
    DECLARE(Bool, hdfs_ignore_file_doesnt_exist, false, R"(
Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.
)", 0) \
    DECLARE(Bool, azure_ignore_file_doesnt_exist, false, R"(
Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.
)", 0) \
    DECLARE(UInt64, azure_sdk_max_retries, 10, R"(
Maximum number of retries in azure sdk
)", 0) \
    DECLARE(UInt64, azure_sdk_retry_initial_backoff_ms, 10, R"(
Minimal backoff between retries in azure sdk
)", 0) \
    DECLARE(UInt64, azure_sdk_retry_max_backoff_ms, 1000, R"(
Maximal backoff between retries in azure sdk
)", 0) \
    DECLARE(Bool, s3_validate_request_settings, true, R"(
Enables s3 request settings validation.

Possible values:
- 1 — validate settings.
- 0 — do not validate settings.
)", 0) \
    DECLARE(Bool, s3_disable_checksum, S3::DEFAULT_DISABLE_CHECKSUM, R"(
Do not calculate a checksum when sending a file to S3. This speeds up writes by avoiding excessive processing passes on a file. It is mostly safe as the data of MergeTree tables is checksummed by ClickHouse anyway, and when S3 is accessed with HTTPS, the TLS layer already provides integrity while transferring through the network. While additional checksums on S3 give defense in depth.
)", 0) \
    DECLARE(UInt64, s3_retry_attempts, S3::DEFAULT_RETRY_ATTEMPTS, R"(
Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries
)", 0) \
    DECLARE(UInt64, s3_request_timeout_ms, S3::DEFAULT_REQUEST_TIMEOUT_MS, R"(
Idleness timeout for sending and receiving data to/from S3. Fail if a single TCP read or write call blocks for this long.
)", 0) \
    DECLARE(UInt64, s3_connect_timeout_ms, S3::DEFAULT_CONNECT_TIMEOUT_MS, R"(
Connection timeout for host from s3 disks.
)", 0) \
    DECLARE(Bool, enable_s3_requests_logging, false, R"(
Enable very explicit logging of S3 requests. Makes sense for debug only.
)", 0) \
    DECLARE(String, s3queue_default_zookeeper_path, "/clickhouse/s3queue/", R"(
Default zookeeper path prefix for S3Queue engine
)", 0) \
    DECLARE(Bool, s3queue_enable_logging_to_s3queue_log, false, R"(
Enable writing to system.s3queue_log. The value can be overwritten per table with table settings
)", 0) \
    DECLARE(UInt64, hdfs_replication, 0, R"(
The actual number of replications can be specified when the hdfs file is created.
)", 0) \
    DECLARE(Bool, hdfs_truncate_on_insert, false, R"(
Enables or disables truncation before an insert in hdfs engine tables. If disabled, an exception will be thrown on an attempt to insert if a file in HDFS already exists.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.
)", 0) \
    DECLARE(Bool, hdfs_create_new_file_on_insert, false, R"(
Enables or disables creating a new file on each insert in HDFS engine tables. If enabled, on each insert a new HDFS file will be created with the name, similar to this pattern:

initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.
)", 0) \
    DECLARE(Bool, hdfs_skip_empty_files, false, R"(
Enables or disables skipping empty files in [HDFS](../../engines/table-engines/integrations/hdfs.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.
)", 0) \
    DECLARE(Bool, azure_skip_empty_files, false, R"(
Enables or disables skipping empty files in S3 engine.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.
)", 0) \
    DECLARE(UInt64, hsts_max_age, 0, R"(
Expired time for HSTS. 0 means disable HSTS.
)", 0) \
    DECLARE(Bool, extremes, false, R"(
Whether to count extreme values (the minimums and maximums in columns of a query result). Accepts 0 or 1. By default, 0 (disabled).
For more information, see the section “Extreme values”.
)", IMPORTANT) \
    DECLARE(Bool, use_uncompressed_cache, false, R"(
Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
Using the uncompressed cache (only for tables in the MergeTree family) can significantly reduce latency and increase throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically to save space for truly small queries. This means that you can keep the ‘use_uncompressed_cache’ setting always set to 1.
)", 0) \
    DECLARE(Bool, replace_running_query, false, R"(
When using the HTTP interface, the ‘query_id’ parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same ‘query_id’ already exists at this time, the behaviour depends on the ‘replace_running_query’ parameter.

`0` (default) – Throw an exception (do not allow the query to run if a query with the same ‘query_id’ is already running).

`1` – Cancel the old query and start running the new one.

Set this parameter to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn’t finished yet, it should be cancelled.
)", 0) \
    DECLARE(UInt64, max_remote_read_network_bandwidth, 0, R"(
The maximum speed of data exchange over the network in bytes per second for read.
)", 0) \
    DECLARE(UInt64, max_remote_write_network_bandwidth, 0, R"(
The maximum speed of data exchange over the network in bytes per second for write.
)", 0) \
    DECLARE(UInt64, max_local_read_bandwidth, 0, R"(
The maximum speed of local reads in bytes per second.
)", 0) \
    DECLARE(UInt64, max_local_write_bandwidth, 0, R"(
The maximum speed of local writes in bytes per second.
)", 0) \
    DECLARE(Bool, stream_like_engine_allow_direct_select, false, R"(
Allow direct SELECT query for Kafka, RabbitMQ, FileLog, Redis Streams, and NATS engines. In case there are attached materialized views, SELECT query is not allowed even if this setting is enabled.
)", 0) \
    DECLARE(String, stream_like_engine_insert_queue, "", R"(
When stream-like engine reads from multiple queues, the user will need to select one queue to insert into when writing. Used by Redis Streams and NATS.
)", 0) \
    DECLARE(Bool, dictionary_validate_primary_key_type, false, R"(
Validate primary key type for dictionaries. By default id type for simple layouts will be implicitly converted to UInt64.
)", 0) \
    DECLARE(Bool, distributed_insert_skip_read_only_replicas, false, R"(
Enables skipping read-only replicas for INSERT queries into Distributed.

Possible values:

- 0 — INSERT was as usual, if it will go to read-only replica it will fail
- 1 — Initiator will skip read-only replicas before sending data to shards.
)", 0) \
    DECLARE(Bool, distributed_foreground_insert, false, R"(
Enables or disables synchronous data insertion into a [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table.

By default, when inserting data into a `Distributed` table, the ClickHouse server sends data to cluster nodes in background mode. When `distributed_foreground_insert=1`, the data is processed synchronously, and the `INSERT` operation succeeds only after all the data is saved on all shards (at least one replica for each shard if `internal_replication` is true).

Possible values:

- 0 — Data is inserted in background mode.
- 1 — Data is inserted in synchronous mode.

Cloud default value: `1`.

**See Also**

- [Distributed Table Engine](../../engines/table-engines/special/distributed.md/#distributed)
- [Managing Distributed Tables](../../sql-reference/statements/system.md/#query-language-system-distributed)
)", 0) ALIAS(insert_distributed_sync) \
    DECLARE(UInt64, distributed_background_insert_timeout, 0, R"(
Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.
)", 0) ALIAS(insert_distributed_timeout) \
    DECLARE(Milliseconds, distributed_background_insert_sleep_time_ms, 100, R"(
Base interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. The actual interval grows exponentially in the event of errors.

Possible values:

- A positive integer number of milliseconds.
)", 0) ALIAS(distributed_directory_monitor_sleep_time_ms) \
    DECLARE(Milliseconds, distributed_background_insert_max_sleep_time_ms, 30000, R"(
Maximum interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. Limits exponential growth of the interval set in the [distributed_background_insert_sleep_time_ms](#distributed_background_insert_sleep_time_ms) setting.

Possible values:

- A positive integer number of milliseconds.
)", 0) ALIAS(distributed_directory_monitor_max_sleep_time_ms) \
    \
    DECLARE(Bool, distributed_background_insert_batch, false, R"(
Enables/disables inserted data sending in batches.

When batch sending is enabled, the [Distributed](../../engines/table-engines/special/distributed.md) table engine tries to send multiple files of inserted data in one operation instead of sending them separately. Batch sending improves cluster performance by better-utilizing server and network resources.

Possible values:

- 1 — Enabled.
- 0 — Disabled.
)", 0) ALIAS(distributed_directory_monitor_batch_inserts) \
    DECLARE(Bool, distributed_background_insert_split_batch_on_failure, false, R"(
Enables/disables splitting batches on failures.

Sometimes sending particular batch to the remote shard may fail, because of some complex pipeline after (i.e. `MATERIALIZED VIEW` with `GROUP BY`) due to `Memory limit exceeded` or similar errors. In this case, retrying will not help (and this will stuck distributed sends for the table) but sending files from that batch one by one may succeed INSERT.

So installing this setting to `1` will disable batching for such batches (i.e. temporary disables `distributed_background_insert_batch` for failed batches).

Possible values:

- 1 — Enabled.
- 0 — Disabled.

:::note
This setting also affects broken batches (that may appears because of abnormal server (machine) termination and no `fsync_after_insert`/`fsync_directories` for [Distributed](../../engines/table-engines/special/distributed.md) table engine).
:::

:::note
You should not rely on automatic batch splitting, since this may hurt performance.
:::
)", 0) ALIAS(distributed_directory_monitor_split_batch_on_failure) \
    \
    DECLARE(Bool, optimize_move_to_prewhere, true, R"(
Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries.

Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables.

Possible values:

- 0 — Automatic `PREWHERE` optimization is disabled.
- 1 — Automatic `PREWHERE` optimization is enabled.
)", 0) \
    DECLARE(Bool, optimize_move_to_prewhere_if_final, false, R"(
Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries with [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier.

Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables.

Possible values:

- 0 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is disabled.
- 1 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is enabled.

**See Also**

- [optimize_move_to_prewhere](#optimize_move_to_prewhere) setting
)", 0) \
    DECLARE(Bool, move_all_conditions_to_prewhere, true, R"(
Move all viable conditions from WHERE to PREWHERE
)", 0) \
    DECLARE(Bool, enable_multiple_prewhere_read_steps, true, R"(
Move more conditions from WHERE to PREWHERE and do reads from disk and filtering in multiple steps if there are multiple conditions combined with AND
)", 0) \
    DECLARE(Bool, move_primary_key_columns_to_end_of_prewhere, true, R"(
Move PREWHERE conditions containing primary key columns to the end of AND chain. It is likely that these conditions are taken into account during primary key analysis and thus will not contribute a lot to PREWHERE filtering.
)", 0) \
    DECLARE(Bool, allow_reorder_prewhere_conditions, true, R"(
When moving conditions from WHERE to PREWHERE, allow reordering them to optimize filtering
)", 0) \
    \
    DECLARE(UInt64, alter_sync, 1, R"(
Allows to set up waiting for actions to be executed on replicas by [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries.

Possible values:

- 0 — Do not wait.
- 1 — Wait for own execution.
- 2 — Wait for everyone.

Cloud default value: `0`.

:::note
`alter_sync` is applicable to `Replicated` tables only, it does nothing to alters of not `Replicated` tables.
:::
)", 0) ALIAS(replication_alter_partitions_sync) \
    DECLARE(Int64, replication_wait_for_inactive_replica_timeout, 120, R"(
Specifies how long (in seconds) to wait for inactive replicas to execute [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries.

Possible values:

- 0 — Do not wait.
- Negative integer — Wait for unlimited time.
- Positive integer — The number of seconds to wait.
)", 0) \
    DECLARE(Bool, alter_move_to_space_execute_async, false, R"(
Execute ALTER TABLE MOVE ... TO [DISK|VOLUME] asynchronously
)", 0) \
    \
    DECLARE(LoadBalancing, load_balancing, LoadBalancing::RANDOM, R"(
Specifies the algorithm of replicas selection that is used for distributed query processing.

ClickHouse supports the following algorithms of choosing replicas:

- [Random](#load_balancing-random) (by default)
- [Nearest hostname](#load_balancing-nearest_hostname)
- [Hostname levenshtein distance](#load_balancing-hostname_levenshtein_distance)
- [In order](#load_balancing-in_order)
- [First or random](#load_balancing-first_or_random)
- [Round robin](#load_balancing-round_robin)

See also:

- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

### Random (by Default) {#load_balancing-random}

``` sql
load_balancing = random
```

The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to anyone of them.
Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data.

### Nearest Hostname {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server’s hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

For instance, example01-01-1 and example01-01-2 are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem primitive, but it does not require external data about network topology, and it does not compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

### Hostname levenshtein distance {#load_balancing-hostname_levenshtein_distance}

``` sql
load_balancing = hostname_levenshtein_distance
```

Just like `nearest_hostname`, but it compares hostname in a [levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) manner. For example:

``` text
example-clickhouse-0-0 ample-clickhouse-0-0
1

example-clickhouse-0-0 example-clickhouse-1-10
2

example-clickhouse-0-0 example-clickhouse-12-0
3
```

### In Order {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Replicas with the same number of errors are accessed in the same order as they are specified in the configuration.
This method is appropriate when you know exactly which replica is preferable.

### First or Random {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

This algorithm chooses the first replica in the set or a random replica if the first is unavailable. It’s effective in cross-replication topology setups, but useless in other configurations.

The `first_or_random` algorithm solves the problem of the `in_order` algorithm. With `in_order`, if one replica goes down, the next one gets a double load while the remaining replicas handle the usual amount of traffic. When using the `first_or_random` algorithm, the load is evenly distributed among replicas that are still available.

It's possible to explicitly define what the first replica is by using the setting `load_balancing_first_offset`. This gives more control to rebalance query workloads among replicas.

### Round Robin {#load_balancing-round_robin}

``` sql
load_balancing = round_robin
```

This algorithm uses a round-robin policy across replicas with the same number of errors (only the queries with `round_robin` policy is accounted).
)", 0) \
    DECLARE(UInt64, load_balancing_first_offset, 0, R"(
Which replica to preferably send a query when FIRST_OR_RANDOM load balancing strategy is used.
)", 0) \
    \
    DECLARE(TotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE, R"(
How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.
See the section “WITH TOTALS modifier”.
)", IMPORTANT) \
    DECLARE(Float, totals_auto_threshold, 0.5, R"(
The threshold for `totals_mode = 'auto'`.
See the section “WITH TOTALS modifier”.
)", 0) \
    \
    DECLARE(Bool, allow_suspicious_low_cardinality_types, false, R"(
Allows or restricts using [LowCardinality](../../sql-reference/data-types/lowcardinality.md) with data types with fixed size of 8 bytes or less: numeric data types and `FixedString(8_bytes_or_less)`.

For small fixed values using of `LowCardinality` is usually inefficient, because ClickHouse stores a numeric index for each row. As a result:

- Disk space usage can rise.
- RAM consumption can be higher, depending on a dictionary size.
- Some functions can work slower due to extra coding/encoding operations.

Merge times in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables can grow due to all the reasons described above.

Possible values:

- 1 — Usage of `LowCardinality` is not restricted.
- 0 — Usage of `LowCardinality` is restricted.
)", 0) \
    DECLARE(Bool, allow_suspicious_fixed_string_types, false, R"(
In CREATE TABLE statement allows creating columns of type FixedString(n) with n > 256. FixedString with length >= 256 is suspicious and most likely indicates a misuse
)", 0) \
    DECLARE(Bool, allow_suspicious_indices, false, R"(
Reject primary/secondary indexes and sorting keys with identical expressions
)", 0) \
    DECLARE(Bool, allow_suspicious_ttl_expressions, false, R"(
Reject TTL expressions that don't depend on any of table's columns. It indicates a user error most of the time.
)", 0) \
    DECLARE(Bool, allow_suspicious_variant_types, false, R"(
In CREATE TABLE statement allows specifying Variant type with similar variant types (for example, with different numeric or date types). Enabling this setting may introduce some ambiguity when working with values with similar types.
)", 0) \
    DECLARE(Bool, allow_suspicious_primary_key, false, R"(
Allow suspicious `PRIMARY KEY`/`ORDER BY` for MergeTree (i.e. SimpleAggregateFunction).
)", 0) \
    DECLARE(Bool, compile_expressions, false, R"(
Compile some scalar functions and operators to native code. Due to a bug in the LLVM compiler infrastructure, on AArch64 machines, it is known to lead to a nullptr dereference and, consequently, server crash. Do not enable this setting.
)", 0) \
    DECLARE(UInt64, min_count_to_compile_expression, 3, R"(
Minimum count of executing same expression before it is get compiled.
)", 0) \
    DECLARE(Bool, compile_aggregate_expressions, true, R"(
Enables or disables JIT-compilation of aggregate functions to native code. Enabling this setting can improve the performance.

Possible values:

- 0 — Aggregation is done without JIT compilation.
- 1 — Aggregation is done using JIT compilation.

**See Also**

- [min_count_to_compile_aggregate_expression](#min_count_to_compile_aggregate_expression)
)", 0) \
    DECLARE(UInt64, min_count_to_compile_aggregate_expression, 3, R"(
The minimum number of identical aggregate expressions to start JIT-compilation. Works only if the [compile_aggregate_expressions](#compile_aggregate_expressions) setting is enabled.

Possible values:

- Positive integer.
- 0 — Identical aggregate expressions are always JIT-compiled.
)", 0) \
    DECLARE(Bool, compile_sort_description, true, R"(
Compile sort description to native code.
)", 0) \
    DECLARE(UInt64, min_count_to_compile_sort_description, 3, R"(
The number of identical sort descriptions before they are JIT-compiled
)", 0) \
    DECLARE(UInt64, group_by_two_level_threshold, 100000, R"(
From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.
)", 0) \
    DECLARE(UInt64, group_by_two_level_threshold_bytes, 50000000, R"(
From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.
)", 0) \
    DECLARE(Bool, distributed_aggregation_memory_efficient, true, R"(
Is the memory-saving mode of distributed aggregation enabled.
)", 0) \
    DECLARE(UInt64, aggregation_memory_efficient_merge_threads, 0, R"(
Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as 'max_threads'.
)", 0) \
    DECLARE(Bool, enable_memory_bound_merging_of_aggregation_results, true, R"(
Enable memory bound merging strategy for aggregation.
)", 0) \
    DECLARE(Bool, enable_positional_arguments, true, R"(
Enables or disables supporting positional arguments for [GROUP BY](../../sql-reference/statements/select/group-by.md), [LIMIT BY](../../sql-reference/statements/select/limit-by.md), [ORDER BY](../../sql-reference/statements/select/order-by.md) statements.

Possible values:

- 0 — Positional arguments aren't supported.
- 1 — Positional arguments are supported: column numbers can use instead of column names.

**Example**

Query:

```sql
CREATE TABLE positional_arguments(one Int, two Int, three Int) ENGINE=Memory();

INSERT INTO positional_arguments VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM positional_arguments ORDER BY 2,3;
```

Result:

```text
┌─one─┬─two─┬─three─┐
│  30 │  10 │   20  │
│  20 │  20 │   10  │
│  10 │  20 │   30  │
└─────┴─────┴───────┘
```
)", 0) \
    DECLARE(Bool, enable_extended_results_for_datetime_functions, false, R"(
Enables or disables returning results of type:
- `Date32` with extended range (compared to type `Date`) for functions [toStartOfYear](../../sql-reference/functions/date-time-functions.md#tostartofyear), [toStartOfISOYear](../../sql-reference/functions/date-time-functions.md#tostartofisoyear), [toStartOfQuarter](../../sql-reference/functions/date-time-functions.md#tostartofquarter), [toStartOfMonth](../../sql-reference/functions/date-time-functions.md#tostartofmonth), [toLastDayOfMonth](../../sql-reference/functions/date-time-functions.md#tolastdayofmonth), [toStartOfWeek](../../sql-reference/functions/date-time-functions.md#tostartofweek), [toLastDayOfWeek](../../sql-reference/functions/date-time-functions.md#tolastdayofweek) and [toMonday](../../sql-reference/functions/date-time-functions.md#tomonday).
- `DateTime64` with extended range (compared to type `DateTime`) for functions [toStartOfDay](../../sql-reference/functions/date-time-functions.md#tostartofday), [toStartOfHour](../../sql-reference/functions/date-time-functions.md#tostartofhour), [toStartOfMinute](../../sql-reference/functions/date-time-functions.md#tostartofminute), [toStartOfFiveMinutes](../../sql-reference/functions/date-time-functions.md#tostartoffiveminutes), [toStartOfTenMinutes](../../sql-reference/functions/date-time-functions.md#tostartoftenminutes), [toStartOfFifteenMinutes](../../sql-reference/functions/date-time-functions.md#tostartoffifteenminutes) and [timeSlot](../../sql-reference/functions/date-time-functions.md#timeslot).

Possible values:

- 0 — Functions return `Date` or `DateTime` for all types of arguments.
- 1 — Functions return `Date32` or `DateTime64` for `Date32` or `DateTime64` arguments and `Date` or `DateTime` otherwise.
)", 0) \
    DECLARE(Bool, allow_nonconst_timezone_arguments, false, R"(
Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*()
)", 0) \
    DECLARE(Bool, function_locate_has_mysql_compatible_argument_order, true, R"(
Controls the order of arguments in function [locate](../../sql-reference/functions/string-search-functions.md#locate).

Possible values:

- 0 — Function `locate` accepts arguments `(haystack, needle[, start_pos])`.
- 1 — Function `locate` accepts arguments `(needle, haystack, [, start_pos])` (MySQL-compatible behavior)
)", 0) \
    \
    DECLARE(Bool, group_by_use_nulls, false, R"(
Changes the way the [GROUP BY clause](/docs/en/sql-reference/statements/select/group-by.md) treats the types of aggregation keys.
When the `ROLLUP`, `CUBE`, or `GROUPING SETS` specifiers are used, some aggregation keys may not be used to produce some result rows.
Columns for these keys are filled with either default value or `NULL` in corresponding rows depending on this setting.

Possible values:

- 0 — The default value for the aggregation key type is used to produce missing values.
- 1 — ClickHouse executes `GROUP BY` the same way as the SQL standard says. The types of aggregation keys are converted to [Nullable](/docs/en/sql-reference/data-types/nullable.md/#data_type-nullable). Columns for corresponding aggregation keys are filled with [NULL](/docs/en/sql-reference/syntax.md) for rows that didn't use it.

See also:

- [GROUP BY clause](/docs/en/sql-reference/statements/select/group-by.md)
)", 0) \
    \
    DECLARE(Bool, skip_unavailable_shards, false, R"(
Enables or disables silently skipping of unavailable shards.

Shard is considered unavailable if all its replicas are unavailable. A replica is unavailable in the following cases:

- ClickHouse can’t connect to replica for any reason.

    When connecting to a replica, ClickHouse performs several attempts. If all these attempts fail, the replica is considered unavailable.

- Replica can’t be resolved through DNS.

    If replica’s hostname can’t be resolved through DNS, it can indicate the following situations:

    - Replica’s host has no DNS record. It can occur in systems with dynamic DNS, for example, [Kubernetes](https://kubernetes.io), where nodes can be unresolvable during downtime, and this is not an error.

    - Configuration error. ClickHouse configuration file contains a wrong hostname.

Possible values:

- 1 — skipping enabled.

    If a shard is unavailable, ClickHouse returns a result based on partial data and does not report node availability issues.

- 0 — skipping disabled.

    If a shard is unavailable, ClickHouse throws an exception.
)", 0) \
    \
    DECLARE(UInt64, parallel_distributed_insert_select, 0, R"(
Enables parallel distributed `INSERT ... SELECT` query.

If we execute `INSERT INTO distributed_table_a SELECT ... FROM distributed_table_b` queries and both tables use the same cluster, and both tables are either [replicated](../../engines/table-engines/mergetree-family/replication.md) or non-replicated, then this query is processed locally on every shard.

Possible values:

- 0 — Disabled.
- 1 — `SELECT` will be executed on each shard from the underlying table of the distributed engine.
- 2 — `SELECT` and `INSERT` will be executed on each shard from/to the underlying table of the distributed engine.
)", 0) \
    DECLARE(UInt64, distributed_group_by_no_merge, 0, R"(
Do not merge aggregation states from different servers for distributed query processing, you can use this in case it is for certain that there are different keys on different shards

Possible values:

- `0` — Disabled (final query processing is done on the initiator node).
- `1` - Do not merge aggregation states from different servers for distributed query processing (query completely processed on the shard, initiator only proxy the data), can be used in case it is for certain that there are different keys on different shards.
- `2` - Same as `1` but applies `ORDER BY` and `LIMIT` (it is not possible when the query processed completely on the remote node, like for `distributed_group_by_no_merge=1`) on the initiator (can be used for queries with `ORDER BY` and/or `LIMIT`).

**Example**

```sql
SELECT *
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY dummy
LIMIT 1
SETTINGS distributed_group_by_no_merge = 1
FORMAT PrettyCompactMonoBlock

┌─dummy─┐
│     0 │
│     0 │
└───────┘
```

```sql
SELECT *
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY dummy
LIMIT 1
SETTINGS distributed_group_by_no_merge = 2
FORMAT PrettyCompactMonoBlock

┌─dummy─┐
│     0 │
└───────┘
```
)", 0) \
    DECLARE(UInt64, distributed_push_down_limit, 1, R"(
Enables or disables [LIMIT](#limit) applying on each shard separately.

This will allow to avoid:
- Sending extra rows over network;
- Processing rows behind the limit on the initiator.

Starting from 21.9 version you cannot get inaccurate results anymore, since `distributed_push_down_limit` changes query execution only if at least one of the conditions met:
- [distributed_group_by_no_merge](#distributed-group-by-no-merge) > 0.
- Query **does not have** `GROUP BY`/`DISTINCT`/`LIMIT BY`, but it has `ORDER BY`/`LIMIT`.
- Query **has** `GROUP BY`/`DISTINCT`/`LIMIT BY` with `ORDER BY`/`LIMIT` and:
    - [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled.
    - [optimize_distributed_group_by_sharding_key](#optimize-distributed-group-by-sharding-key) is enabled.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [distributed_group_by_no_merge](#distributed-group-by-no-merge)
- [optimize_skip_unused_shards](#optimize-skip-unused-shards)
- [optimize_distributed_group_by_sharding_key](#optimize-distributed-group-by-sharding-key)
)", 0) \
    DECLARE(Bool, optimize_distributed_group_by_sharding_key, true, R"(
Optimize `GROUP BY sharding_key` queries, by avoiding costly aggregation on the initiator server (which will reduce memory usage for the query on the initiator server).

The following types of queries are supported (and all combinations of them):

- `SELECT DISTINCT [..., ]sharding_key[, ...] FROM dist`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...]`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] ORDER BY x`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1 BY x`

The following types of queries are not supported (support for some of them may be added later):

- `SELECT ... GROUP BY sharding_key[, ...] WITH TOTALS`
- `SELECT ... GROUP BY sharding_key[, ...] WITH ROLLUP`
- `SELECT ... GROUP BY sharding_key[, ...] WITH CUBE`
- `SELECT ... GROUP BY sharding_key[, ...] SETTINGS extremes=1`

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [distributed_group_by_no_merge](#distributed-group-by-no-merge)
- [distributed_push_down_limit](#distributed-push-down-limit)
- [optimize_skip_unused_shards](#optimize-skip-unused-shards)

:::note
Right now it requires `optimize_skip_unused_shards` (the reason behind this is that one day it may be enabled by default, and it will work correctly only if data was inserted via Distributed table, i.e. data is distributed according to sharding_key).
:::
)", 0) \
    DECLARE(UInt64, optimize_skip_unused_shards_limit, 1000, R"(
Limit for number of sharding key values, turns off `optimize_skip_unused_shards` if the limit is reached.

Too many values may require significant amount for processing, while the benefit is doubtful, since if you have huge number of values in `IN (...)`, then most likely the query will be sent to all shards anyway.
)", 0) \
    DECLARE(Bool, optimize_skip_unused_shards, false, R"(
Enables or disables skipping of unused shards for [SELECT](../../sql-reference/statements/select/index.md) queries that have sharding key condition in `WHERE/PREWHERE` (assuming that the data is distributed by sharding key, otherwise a query yields incorrect result).

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, optimize_skip_unused_shards_rewrite_in, true, R"(
Rewrite IN in query for remote shards to exclude values that does not belong to the shard (requires optimize_skip_unused_shards).

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, allow_nondeterministic_optimize_skip_unused_shards, false, R"(
Allow nondeterministic (like `rand` or `dictGet`, since later has some caveats with updates) functions in sharding key.

Possible values:

- 0 — Disallowed.
- 1 — Allowed.
)", 0) \
    DECLARE(UInt64, force_optimize_skip_unused_shards, 0, R"(
Enables or disables query execution if [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled, an exception will be thrown.

Possible values:

- 0 — Disabled. ClickHouse does not throw an exception.
- 1 — Enabled. Query execution is disabled only if the table has a sharding key.
- 2 — Enabled. Query execution is disabled regardless of whether a sharding key is defined for the table.
)", 0) \
    DECLARE(UInt64, optimize_skip_unused_shards_nesting, 0, R"(
Controls [`optimize_skip_unused_shards`](#optimize-skip-unused-shards) (hence still requires [`optimize_skip_unused_shards`](#optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

- 0 — Disabled, `optimize_skip_unused_shards` works always.
- 1 — Enables `optimize_skip_unused_shards` only for the first level.
- 2 — Enables `optimize_skip_unused_shards` up to the second level.
)", 0) \
    DECLARE(UInt64, force_optimize_skip_unused_shards_nesting, 0, R"(
Controls [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards) (hence still requires [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

- 0 - Disabled, `force_optimize_skip_unused_shards` works always.
- 1 — Enables `force_optimize_skip_unused_shards` only for the first level.
- 2 — Enables `force_optimize_skip_unused_shards` up to the second level.
)", 0) \
    \
    DECLARE(Bool, input_format_parallel_parsing, true, R"(
Enables or disables order-preserving parallel parsing of data formats. Supported only for [TSV](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [CSV](../../interfaces/formats.md/#csv) and [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) formats.

Possible values:

- 1 — Enabled.
- 0 — Disabled.
)", 0) \
    DECLARE(UInt64, min_chunk_bytes_for_parallel_parsing, (10 * 1024 * 1024), R"(
- Type: unsigned int
- Default value: 1 MiB

The minimum chunk size in bytes, which each thread will parse in parallel.
)", 0) \
    DECLARE(Bool, output_format_parallel_formatting, true, R"(
Enables or disables parallel formatting of data formats. Supported only for [TSV](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [CSV](../../interfaces/formats.md/#csv) and [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) formats.

Possible values:

- 1 — Enabled.
- 0 — Disabled.
)", 0) \
    DECLARE(UInt64, output_format_compression_level, 3, R"(
Default compression level if query output is compressed. The setting is applied when `SELECT` query has `INTO OUTFILE` or when writing to table functions `file`, `url`, `hdfs`, `s3`, or `azureBlobStorage`.

Possible values: from `1` to `22`
)", 0) \
    DECLARE(UInt64, output_format_compression_zstd_window_log, 0, R"(
Can be used when the output compression method is `zstd`. If greater than `0`, this setting explicitly sets compression window size (power of `2`) and enables a long-range mode for zstd compression. This can help to achieve a better compression ratio.

Possible values: non-negative numbers. Note that if the value is too small or too big, `zstdlib` will throw an exception. Typical values are from `20` (window size = `1MB`) to `30` (window size = `1GB`).
)", 0) \
    DECLARE(Bool, enable_parsing_to_custom_serialization, true, R"(
If true then data can be parsed directly to columns with custom serialization (e.g. Sparse) according to hints for serialization got from the table.
)", 0) \
    \
    DECLARE(UInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192), R"(
If the number of rows to be read from a file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file on several threads.

Possible values:

- Positive integer.
)", 0) \
    DECLARE(UInt64, merge_tree_min_bytes_for_concurrent_read, (24 * 10 * 1024 * 1024), R"(
If the number of bytes to read from one file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine table exceeds `merge_tree_min_bytes_for_concurrent_read`, then ClickHouse tries to concurrently read from this file in several threads.

Possible value:

- Positive integer.
)", 0) \
    DECLARE(UInt64, merge_tree_min_rows_for_seek, 0, R"(
If the distance between two data blocks to be read in one file is less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file but reads the data sequentially.

Possible values:

- Any positive integer.
)", 0) \
    DECLARE(UInt64, merge_tree_min_bytes_for_seek, 0, R"(
If the distance between two data blocks to be read in one file is less than `merge_tree_min_bytes_for_seek` bytes, then ClickHouse sequentially reads a range of file that contains both blocks, thus avoiding extra seek.

Possible values:

- Any positive integer.
)", 0) \
    DECLARE(UInt64, merge_tree_coarse_index_granularity, 8, R"(
When searching for data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range into `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively.

Possible values:

- Any positive even integer.
)", 0) \
    DECLARE(UInt64, merge_tree_max_rows_to_use_cache, (128 * 8192), R"(
If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it does not use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

- Any positive integer.
)", 0) \
    DECLARE(UInt64, merge_tree_max_bytes_to_use_cache, (192 * 10 * 1024 * 1024), R"(
If ClickHouse should read more than `merge_tree_max_bytes_to_use_cache` bytes in one query, it does not use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

- Any positive integer.
)", 0) \
    DECLARE(Bool, do_not_merge_across_partitions_select_final, false, R"(
Merge parts only in one partition in select final
)", 0) \
    DECLARE(Bool, split_parts_ranges_into_intersecting_and_non_intersecting_final, true, R"(
Split parts ranges into intersecting and non intersecting during FINAL optimization
)", 0) \
    DECLARE(Bool, split_intersecting_parts_ranges_into_layers_final, true, R"(
Split intersecting parts ranges into layers during FINAL optimization
)", 0) \
    \
    DECLARE(UInt64, mysql_max_rows_to_insert, 65536, R"(
The maximum number of rows in MySQL batch insertion of the MySQL storage engine
)", 0) \
    DECLARE(Bool, mysql_map_string_to_text_in_show_columns, true, R"(
When enabled, [String](../../sql-reference/data-types/string.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md#show_columns).

Has an effect only when the connection is made through the MySQL wire protocol.

- 0 - Use `BLOB`.
- 1 - Use `TEXT`.
)", 0) \
    DECLARE(Bool, mysql_map_fixed_string_to_text_in_show_columns, true, R"(
When enabled, [FixedString](../../sql-reference/data-types/fixedstring.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md#show_columns).

Has an effect only when the connection is made through the MySQL wire protocol.

- 0 - Use `BLOB`.
- 1 - Use `TEXT`.
)", 0) \
    \
    DECLARE(UInt64, optimize_min_equality_disjunction_chain_length, 3, R"(
The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization
)", 0) \
    DECLARE(UInt64, optimize_min_inequality_conjunction_chain_length, 3, R"(
The minimum length of the expression `expr <> x1 AND ... expr <> xN` for optimization
)", 0) \
    \
    DECLARE(UInt64, min_bytes_to_use_direct_io, 0, R"(
The minimum data volume required for using direct I/O access to the storage disk.

ClickHouse uses this setting when reading data from tables. If the total storage volume of all the data to be read exceeds `min_bytes_to_use_direct_io` bytes, then ClickHouse reads the data from the storage disk with the `O_DIRECT` option.

Possible values:

- 0 — Direct I/O is disabled.
- Positive integer.
)", 0) \
    DECLARE(UInt64, min_bytes_to_use_mmap_io, 0, R"(
This is an experimental setting. Sets the minimum amount of memory for reading large files without copying data from the kernel to userspace. Recommended threshold is about 64 MB, because [mmap/munmap](https://en.wikipedia.org/wiki/Mmap) is slow. It makes sense only for large files and helps only if data reside in the page cache.

Possible values:

- Positive integer.
- 0 — Big files read with only copying data from kernel to userspace.
)", 0) \
    DECLARE(Bool, checksum_on_read, true, R"(
Validate checksums on reading. It is enabled by default and should be always enabled in production. Please do not expect any benefits in disabling this setting. It may only be used for experiments and benchmarks. The setting is only applicable for tables of MergeTree family. Checksums are always validated for other table engines and when receiving data over the network.
)", 0) \
    \
    DECLARE(Bool, force_index_by_date, false, R"(
Disables query execution if the index can’t be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
)", 0) \
    DECLARE(Bool, force_primary_key, false, R"(
Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For more information about data ranges in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
)", 0) \
    DECLARE(Bool, use_skip_indexes, true, R"(
Use data skipping indexes during query execution.

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, use_skip_indexes_if_final, false, R"(
Controls whether skipping indexes are used when executing a query with the FINAL modifier.

By default, this setting is disabled because skip indexes may exclude rows (granules) containing the latest data, which could lead to incorrect results. When enabled, skipping indexes are applied even with the FINAL modifier, potentially improving performance but with the risk of missing recent updates.

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, materialize_skip_indexes_on_insert, true, R"(
If true skip indexes are calculated on inserts, otherwise skip indexes will be calculated only during merges
)", 0) \
    DECLARE(Bool, materialize_statistics_on_insert, true, R"(
If true statistics are calculated on inserts, otherwise statistics will be calculated only during merges
)", 0) \
    DECLARE(String, ignore_data_skipping_indices, "", R"(
Ignores the skipping indexes specified if used by the query.

Consider the following example:

```sql
CREATE TABLE data
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1,
    INDEX y_idx y TYPE minmax GRANULARITY 1,
    INDEX xy_idx (x,y) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO data VALUES (1, 2, 3);

SELECT * FROM data;
SELECT * FROM data SETTINGS ignore_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error.
SELECT * FROM data SETTINGS ignore_data_skipping_indices='x_idx'; -- Ok.
SELECT * FROM data SETTINGS ignore_data_skipping_indices='na_idx'; -- Ok.

SELECT * FROM data WHERE x = 1 AND y = 1 SETTINGS ignore_data_skipping_indices='xy_idx',force_data_skipping_indices='xy_idx' ; -- query will produce INDEX_NOT_USED error, since xy_idx is explicitly ignored.
SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';
```

The query without ignoring any indexes:
```sql
EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2;

Expression ((Projection + Before ORDER BY))
  Filter (WHERE)
    ReadFromMergeTree (default.data)
    Indexes:
      PrimaryKey
        Condition: true
        Parts: 1/1
        Granules: 1/1
      Skip
        Name: x_idx
        Description: minmax GRANULARITY 1
        Parts: 0/1
        Granules: 0/1
      Skip
        Name: y_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
      Skip
        Name: xy_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
```

Ignoring the `xy_idx` index:
```sql
EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';

Expression ((Projection + Before ORDER BY))
  Filter (WHERE)
    ReadFromMergeTree (default.data)
    Indexes:
      PrimaryKey
        Condition: true
        Parts: 1/1
        Granules: 1/1
      Skip
        Name: x_idx
        Description: minmax GRANULARITY 1
        Parts: 0/1
        Granules: 0/1
      Skip
        Name: y_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
```

Works with tables in the MergeTree family.
)", 0) \
    \
    DECLARE(String, force_data_skipping_indices, "", R"(
Disables query execution if passed data skipping indices wasn't used.

Consider the following example:

```sql
CREATE TABLE data
(
    key Int,
    d1 Int,
    d1_null Nullable(Int),
    INDEX d1_idx d1 TYPE minmax GRANULARITY 1,
    INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

SELECT * FROM data_01515;
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error.
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_idx'; -- query will produce INDEX_NOT_USED error.
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx'; -- Ok.
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`'; -- Ok (example of full featured parser).
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- query will produce INDEX_NOT_USED error, since d1_null_idx is not used.
SELECT * FROM data_01515 WHERE d1 = 0 AND assumeNotNull(d1_null) = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- Ok.
```
)", 0) \
    \
    DECLARE(Float, max_streams_to_max_threads_ratio, 1, R"(
Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.
)", 0) \
    DECLARE(Float, max_streams_multiplier_for_merge_tables, 5, R"(
Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and is especially helpful when merged tables differ in size.
)", 0) \
    \
    DECLARE(String, network_compression_method, "LZ4", R"(
Sets the method of data compression that is used for communication between servers and between server and [clickhouse-client](../../interfaces/cli.md).

Possible values:

- `LZ4` — sets LZ4 compression method.
- `ZSTD` — sets ZSTD compression method.

**See Also**

- [network_zstd_compression_level](#network_zstd_compression_level)
)", 0) \
    \
    DECLARE(Int64, network_zstd_compression_level, 1, R"(
Adjusts the level of ZSTD compression. Used only when [network_compression_method](#network_compression_method) is set to `ZSTD`.

Possible values:

- Positive integer from 1 to 15.
)", 0) \
    \
    DECLARE(Int64, zstd_window_log_max, 0, R"(
Allows you to select the max window log of ZSTD (it will not be used for MergeTree family)
)", 0) \
    \
    DECLARE(UInt64, priority, 0, R"(
Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.
)", 0) \
    DECLARE(Int64, os_thread_priority, 0, R"(
Sets the priority ([nice](https://en.wikipedia.org/wiki/Nice_(Unix))) for threads that execute queries. The OS scheduler considers this priority when choosing the next thread to run on each available CPU core.

:::note
To use this setting, you need to set the `CAP_SYS_NICE` capability. The `clickhouse-server` package sets it up during installation. Some virtual environments do not allow you to set the `CAP_SYS_NICE` capability. In this case, `clickhouse-server` shows a message about it at the start.
:::

Possible values:

- You can set values in the range `[-20, 19]`.

Lower values mean higher priority. Threads with low `nice` priority values are executed more frequently than threads with high values. High values are preferable for long-running non-interactive queries because it allows them to quickly give up resources in favour of short interactive queries when they arrive.
)", 0) \
    \
    DECLARE(Bool, log_queries, true, R"(
Setting up query logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../../operations/server-configuration-parameters/settings.md/#query-log) server configuration parameter.

Example:

``` text
log_queries=1
```
)", 0) \
    DECLARE(Bool, log_formatted_queries, false, R"(
Allows to log formatted queries to the [system.query_log](../../operations/system-tables/query_log.md) system table (populates `formatted_query` column in the [system.query_log](../../operations/system-tables/query_log.md)).

Possible values:

- 0 — Formatted queries are not logged in the system table.
- 1 — Formatted queries are logged in the system table.
)", 0) \
    DECLARE(LogQueriesType, log_queries_min_type, QueryLogElementType::QUERY_START, R"(
`query_log` minimal type to log.

Possible values:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Can be used to limit which entities will go to `query_log`, say you are interested only in errors, then you can use `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```
)", 0) \
    DECLARE(Milliseconds, log_queries_min_query_duration_ms, 0, R"(
If enabled (non-zero), queries faster than the value of this setting will not be logged (you can think about this as a `long_query_time` for [MySQL Slow Query Log](https://dev.mysql.com/doc/refman/5.7/en/slow-query-log.html)), and this basically means that you will not find them in the following tables:

- `system.query_log`
- `system.query_thread_log`

Only the queries with the following type will get to the log:

- `QUERY_FINISH`
- `EXCEPTION_WHILE_PROCESSING`

- Type: milliseconds
- Default value: 0 (any query)
)", 0) \
    DECLARE(UInt64, log_queries_cut_to_length, 100000, R"(
If query length is greater than a specified threshold (in bytes), then cut query when writing to query log. Also limit the length of printed query in ordinary text log.
)", 0) \
    DECLARE(Float, log_queries_probability, 1., R"(
Allows a user to write to [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), and [query_views_log](../../operations/system-tables/query_views_log.md) system tables only a sample of queries selected randomly with the specified probability. It helps to reduce the load with a large volume of queries in a second.

Possible values:

- 0 — Queries are not logged in the system tables.
- Positive floating-point number in the range [0..1]. For example, if the setting value is `0.5`, about half of the queries are logged in the system tables.
- 1 — All queries are logged in the system tables.
)", 0) \
    \
    DECLARE(Bool, log_processors_profiles, true, R"(
Write time that processor spent during execution/waiting for data to `system.processors_profile_log` table.

See also:

- [`system.processors_profile_log`](../../operations/system-tables/processors_profile_log.md)
- [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)
)", 0) \
    DECLARE(DistributedProductMode, distributed_product_mode, DistributedProductMode::DENY, R"(
Changes the behaviour of [distributed subqueries](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restrictions:

- Only applied for IN and JOIN subqueries.
- Only if the FROM section uses a distributed table containing more than one shard.
- If the subquery concerns a distributed table containing more than one shard.
- Not used for a table-valued [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

- `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” exception).
- `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
- `global` — Replaces the `IN`/`JOIN` query with `GLOBAL IN`/`GLOBAL JOIN.`
- `allow` — Allows the use of these types of subqueries.
)", IMPORTANT) \
    \
    DECLARE(UInt64, max_concurrent_queries_for_all_users, 0, R"(
Throw exception if the value of this setting is less or equal than the current number of simultaneously processed queries.

Example: `max_concurrent_queries_for_all_users` can be set to 99 for all users and database administrator can set it to 100 for itself to run queries for investigation even when the server is overloaded.

Modifying the setting for one query or user does not affect other queries.

Possible values:

- Positive integer.
- 0 — No limit.

**Example**

``` xml
<max_concurrent_queries_for_all_users>99</max_concurrent_queries_for_all_users>
```

**See Also**

- [max_concurrent_queries](/docs/en/operations/server-configuration-parameters/settings.md/#max_concurrent_queries)
)", 0) \
    DECLARE(UInt64, max_concurrent_queries_for_user, 0, R"(
The maximum number of simultaneously processed queries per user.

Possible values:

- Positive integer.
- 0 — No limit.

**Example**

``` xml
<max_concurrent_queries_for_user>5</max_concurrent_queries_for_user>
```
)", 0) \
    \
    DECLARE(Bool, insert_deduplicate, true, R"(
Enables or disables block deduplication of `INSERT` (for Replicated\* tables).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

By default, blocks inserted into replicated tables by the `INSERT` statement are deduplicated (see [Data Replication](../../engines/table-engines/mergetree-family/replication.md)).
For the replicated tables by default the only 100 of the most recent blocks for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated-deduplication-window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated-deduplication-window-seconds)).
For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non-replicated-deduplication-window).
)", 0) \
    DECLARE(Bool, async_insert_deduplicate, false, R"(
For async INSERT queries in the replicated table, specifies that deduplication of inserting blocks should be performed
)", 0) \
    \
    DECLARE(UInt64Auto, insert_quorum, 0, R"(
:::note
This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information.
:::

Enables the quorum writes.

- If `insert_quorum < 2`, the quorum writes are disabled.
- If `insert_quorum >= 2`, the quorum writes are enabled.
- If `insert_quorum = 'auto'`, use majority number (`number_of_replicas / 2 + 1`) as quorum number.

Quorum writes

`INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written.

When `insert_quorum_parallel` is disabled, all replicas in the quorum are consistent, i.e. they contain data from all previous `INSERT` queries (the `INSERT` sequence is linearized). When reading data written using `insert_quorum` and `insert_quorum_parallel` is disabled, you can turn on sequential consistency for `SELECT` queries using [select_sequential_consistency](#select_sequential_consistency).

ClickHouse generates an exception:

- If the number of available replicas at the time of the query is less than the `insert_quorum`.
- When `insert_quorum_parallel` is disabled and an attempt to write data is made when the previous block has not yet been inserted in `insert_quorum` of replicas. This situation may occur if the user tries to perform another `INSERT` query to the same table before the previous one with `insert_quorum` is completed.

See also:

- [insert_quorum_timeout](#insert_quorum_timeout)
- [insert_quorum_parallel](#insert_quorum_parallel)
- [select_sequential_consistency](#select_sequential_consistency)
)", 0) \
    DECLARE(Milliseconds, insert_quorum_timeout, 600000, R"(
Write to a quorum timeout in milliseconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_parallel](#insert_quorum_parallel)
- [select_sequential_consistency](#select_sequential_consistency)
)", 0) \
    DECLARE(Bool, insert_quorum_parallel, true, R"(
:::note
This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information.
:::

Enables or disables parallelism for quorum `INSERT` queries. If enabled, additional `INSERT` queries can be sent while previous queries have not yet finished. If disabled, additional writes to the same table will be rejected.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_timeout](#insert_quorum_timeout)
- [select_sequential_consistency](#select_sequential_consistency)
)", 0) \
    DECLARE(UInt64, select_sequential_consistency, 0, R"(
:::note
This setting differ in behavior between SharedMergeTree and ReplicatedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information about the behavior of `select_sequential_consistency` in SharedMergeTree.
:::

Enables or disables sequential consistency for `SELECT` queries. Requires `insert_quorum_parallel` to be disabled (enabled by default).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Usage

When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas.

When `insert_quorum_parallel` is enabled (the default), then `select_sequential_consistency` does not work. This is because parallel `INSERT` queries can be written to different sets of quorum replicas so there is no guarantee a single replica will have received all writes.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_timeout](#insert_quorum_timeout)
- [insert_quorum_parallel](#insert_quorum_parallel)
)", 0) \
    DECLARE(UInt64, table_function_remote_max_addresses, 1000, R"(
Sets the maximum number of addresses generated from patterns for the [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

- Positive integer.
)", 0) \
    DECLARE(Milliseconds, read_backoff_min_latency_ms, 1000, R"(
Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.
)", 0) \
    DECLARE(UInt64, read_backoff_max_throughput, 1048576, R"(
Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.
)", 0) \
    DECLARE(Milliseconds, read_backoff_min_interval_between_events_ms, 1000, R"(
Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.
)", 0) \
    DECLARE(UInt64, read_backoff_min_events, 2, R"(
Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.
)", 0) \
    \
    DECLARE(UInt64, read_backoff_min_concurrency, 1, R"(
Settings to try keeping the minimal number of threads in case of slow reads.
)", 0) \
    \
    DECLARE(Float, memory_tracker_fault_probability, 0., R"(
For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.
)", 0) \
    DECLARE(Float, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability, 0.0, R"(
For testing of `PartsSplitter` - split read ranges into intersecting and non intersecting every time you read from MergeTree with the specified probability.
)", 0) \
    \
    DECLARE(Bool, enable_http_compression, false, R"(
Enables or disables data compression in the response to an HTTP request.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Int64, http_zlib_compression_level, 3, R"(
Sets the level of data compression in the response to an HTTP request if [enable_http_compression = 1](#enable_http_compression).

Possible values: Numbers from 1 to 9.
)", 0) \
    \
    DECLARE(Bool, http_native_compression_disable_checksumming_on_decompress, false, R"(
Enables or disables checksum verification when decompressing the HTTP POST data from the client. Used only for ClickHouse native compression format (not used with `gzip` or `deflate`).

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    \
    DECLARE(String, count_distinct_implementation, "uniqExact", R"(
Specifies which of the `uniq*` functions should be used to perform the [COUNT(DISTINCT ...)](../../sql-reference/aggregate-functions/reference/count.md/#agg_function-count) construction.

Possible values:

- [uniq](../../sql-reference/aggregate-functions/reference/uniq.md/#agg_function-uniq)
- [uniqCombined](../../sql-reference/aggregate-functions/reference/uniqcombined.md/#agg_function-uniqcombined)
- [uniqCombined64](../../sql-reference/aggregate-functions/reference/uniqcombined64.md/#agg_function-uniqcombined64)
- [uniqHLL12](../../sql-reference/aggregate-functions/reference/uniqhll12.md/#agg_function-uniqhll12)
- [uniqExact](../../sql-reference/aggregate-functions/reference/uniqexact.md/#agg_function-uniqexact)
)", 0) \
    \
    DECLARE(Bool, add_http_cors_header, false, R"(
Write add http CORS header.
)", 0) \
    \
    DECLARE(UInt64, max_http_get_redirects, 0, R"(
Max number of HTTP GET redirects hops allowed. Ensures additional security measures are in place to prevent a malicious server from redirecting your requests to unexpected services.\n\nIt is the case when an external server redirects to another address, but that address appears to be internal to the company's infrastructure, and by sending an HTTP request to an internal server, you could request an internal API from the internal network, bypassing the auth, or even query other services, such as Redis or Memcached. When you don't have an internal infrastructure (including something running on your localhost), or you trust the server, it is safe to allow redirects. Although keep in mind, that if the URL uses HTTP instead of HTTPS, and you will have to trust not only the remote server but also your ISP and every network in the middle.
)", 0) \
    \
    DECLARE(Bool, use_client_time_zone, false, R"(
Use client timezone for interpreting DateTime string values, instead of adopting server timezone.
)", 0) \
    \
    DECLARE(Bool, send_progress_in_http_headers, false, R"(
Enables or disables `X-ClickHouse-Progress` HTTP response headers in `clickhouse-server` responses.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    \
    DECLARE(UInt64, http_headers_progress_interval_ms, 100, R"(
Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.
)", 0) \
    DECLARE(Bool, http_wait_end_of_query, false, R"(
Enable HTTP response buffering on the server-side.
)", 0) \
    DECLARE(Bool, http_write_exception_in_output_format, true, R"(
Write exception in output format to produce valid output. Works with JSON and XML formats.
)", 0) \
    DECLARE(UInt64, http_response_buffer_size, 0, R"(
The number of bytes to buffer in the server memory before sending a HTTP response to the client or flushing to disk (when http_wait_end_of_query is enabled).
)", 0) \
    \
    DECLARE(Bool, fsync_metadata, true, R"(
Enables or disables [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) when writing `.sql` files. Enabled by default.

It makes sense to disable it if the server has millions of tiny tables that are constantly being created and destroyed.
)", 0)    \
    \
    DECLARE(Bool, join_use_nulls, false, R"(
Sets the type of [JOIN](../../sql-reference/statements/select/join.md) behaviour. When merging tables, empty cells may appear. ClickHouse fills them differently based on this setting.

Possible values:

- 0 — The empty cells are filled with the default value of the corresponding field type.
- 1 — `JOIN` behaves the same way as in standard SQL. The type of the corresponding field is converted to [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable), and empty cells are filled with [NULL](../../sql-reference/syntax.md).
)", IMPORTANT) \
    \
    DECLARE(UInt64, join_output_by_rowlist_perkey_rows_threshold, 5, R"(
The lower limit of per-key average rows in the right table to determine whether to output by row list in hash join.
)", 0) \
    DECLARE(JoinStrictness, join_default_strictness, JoinStrictness::All, R"(
Sets default strictness for [JOIN clauses](../../sql-reference/statements/select/join.md/#select-join).

Possible values:

- `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the normal `JOIN` behaviour from standard SQL.
- `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same.
- `ASOF` — For joining sequences with an uncertain match.
- `Empty string` — If `ALL` or `ANY` is not specified in the query, ClickHouse throws an exception.
)", 0) \
    DECLARE(Bool, any_join_distinct_right_table_keys, false, R"(
Enables legacy ClickHouse server behaviour in `ANY INNER|LEFT JOIN` operations.

:::note
Use this setting only for backward compatibility if your use cases depend on legacy `JOIN` behaviour.
:::

When the legacy behaviour is enabled:

- Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are not equal because ClickHouse uses the logic with many-to-one left-to-right table keys mapping.
- Results of `ANY INNER JOIN` operations contain all rows from the left table like the `SEMI LEFT JOIN` operations do.

When the legacy behaviour is disabled:

- Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are equal because ClickHouse uses the logic which provides one-to-many keys mapping in `ANY RIGHT JOIN` operations.
- Results of `ANY INNER JOIN` operations contain one row per key from both the left and right tables.

Possible values:

- 0 — Legacy behaviour is disabled.
- 1 — Legacy behaviour is enabled.

See also:

- [JOIN strictness](../../sql-reference/statements/select/join.md/#join-settings)
)", IMPORTANT) \
    DECLARE(Bool, single_join_prefer_left_table, true, R"(
For single JOIN in case of identifier ambiguity prefer left table
)", IMPORTANT) \
    \
    DECLARE(JoinInnerTableSelectionMode, query_plan_join_inner_table_selection, JoinInnerTableSelectionMode::Auto, R"(
Select the side of the join to be the inner table in the query plan. Supported only for `ALL` join strictness with `JOIN ON` clause. Possible values: 'auto', 'left', 'right'.
)", 0) \
    DECLARE(UInt64, preferred_block_size_bytes, 1000000, R"(
This setting adjusts the data block size for query processing and represents additional fine-tuning to the more rough 'max_block_size' setting. If the columns are large and with 'max_block_size' rows the block size is likely to be larger than the specified amount of bytes, its size will be lowered for better CPU cache locality.
)", 0) \
    \
    DECLARE(UInt64, max_replica_delay_for_distributed_queries, 300, R"(
Disables lagging replicas for distributed queries. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

Sets the time in seconds. If a replica's lag is greater than or equal to the set value, this replica is not used.

Possible values:

- Positive integer.
- 0 — Replica lags are not checked.

To prevent the use of any replica with a non-zero lag, set this parameter to 1.

Used when performing `SELECT` from a distributed table that points to replicated tables.
)", 0) \
    DECLARE(Bool, fallback_to_stale_replicas_for_distributed_queries, true, R"(
Forces a query to an out-of-date replica if updated data is not available. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing `SELECT` from a distributed table that points to replicated tables.

By default, 1 (enabled).
)", 0) \
    DECLARE(UInt64, preferred_max_column_in_block_size_bytes, 0, R"(
Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.
)", 0) \
    \
    DECLARE(UInt64, parts_to_delay_insert, 0, R"(
If the destination table contains at least that many active parts in a single partition, artificially slow down insert into table.
)", 0) \
    DECLARE(UInt64, parts_to_throw_insert, 0, R"(
If more than this number active parts in a single partition of the destination table, throw 'Too many parts ...' exception.
)", 0) \
    DECLARE(UInt64, number_of_mutations_to_delay, 0, R"(
If the mutated table contains at least that many unfinished mutations, artificially slow down mutations of table. 0 - disabled
)", 0) \
    DECLARE(UInt64, number_of_mutations_to_throw, 0, R"(
If the mutated table contains at least that many unfinished mutations, throw 'Too many mutations ...' exception. 0 - disabled
)", 0) \
    DECLARE(Int64, distributed_ddl_task_timeout, 180, R"(
Sets timeout for DDL query responses from all hosts in cluster. If a DDL request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite.

Possible values:

- Positive integer.
- 0 — Async mode.
- Negative integer — infinite timeout.
)", 0) \
    DECLARE(Milliseconds, stream_flush_interval_ms, 7500, R"(
Works for tables with streaming in the case of a timeout, or when a thread generates [max_insert_block_size](#max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.
)", 0) \
    DECLARE(Milliseconds, stream_poll_timeout_ms, 500, R"(
Timeout for polling data from/to streaming storages.
)", 0) \
    DECLARE(UInt64, min_free_disk_bytes_to_perform_insert, 0, R"(
Minimum free disk space bytes to perform an insert.
)", 0) \
    DECLARE(Float, min_free_disk_ratio_to_perform_insert, 0.0, R"(
Minimum free disk space ratio to perform an insert.
)", 0) \
    \
    DECLARE(Bool, final, false, R"(
Automatically applies [FINAL](../../sql-reference/statements/select/from.md#final-modifier) modifier to all tables in a query, to tables where [FINAL](../../sql-reference/statements/select/from.md#final-modifier) is applicable, including joined tables and tables in sub-queries, and
distributed tables.

Possible values:

- 0 - disabled
- 1 - enabled

Example:

```sql
CREATE TABLE test
(
    key Int64,
    some String
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO test FORMAT Values (1, 'first');
INSERT INTO test FORMAT Values (1, 'second');

SELECT * FROM test;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘
┌─key─┬─some──┐
│   1 │ first │
└─────┴───────┘

SELECT * FROM test SETTINGS final = 1;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘

SET final = 1;
SELECT * FROM test;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘
```
)", 0) \
    \
    DECLARE(Bool, partial_result_on_first_cancel, false, R"(
Allows query to return a partial result after cancel.
)", 0) \
    \
    DECLARE(Bool, ignore_on_cluster_for_replicated_udf_queries, false, R"(
Ignore ON CLUSTER clause for replicated UDF management queries.
)", 0) \
    DECLARE(Bool, ignore_on_cluster_for_replicated_access_entities_queries, false, R"(
Ignore ON CLUSTER clause for replicated access entities management queries.
)", 0) \
    DECLARE(Bool, ignore_on_cluster_for_replicated_named_collections_queries, false, R"(
Ignore ON CLUSTER clause for replicated named collections management queries.
)", 0) \
    /** Settings for testing hedged requests */ \
    DECLARE(Milliseconds, sleep_in_send_tables_status_ms, 0, R"(
Time to sleep in sending tables status response in TCPHandler
)", 0) \
    DECLARE(Milliseconds, sleep_in_send_data_ms, 0, R"(
Time to sleep in sending data in TCPHandler
)", 0) \
    DECLARE(Milliseconds, sleep_after_receiving_query_ms, 0, R"(
Time to sleep after receiving query in TCPHandler
)", 0) \
    DECLARE(UInt64, unknown_packet_in_send_data, 0, R"(
Send unknown packet instead of data Nth data packet
)", 0) \
    \
    DECLARE(Bool, insert_allow_materialized_columns, false, R"(
If setting is enabled, Allow materialized columns in INSERT.
)", 0) \
    DECLARE(Seconds, http_connection_timeout, DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, R"(
HTTP connection timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).
)", 0) \
    DECLARE(Seconds, http_send_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, R"(
HTTP send timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

:::note
It's applicable only to the default profile. A server reboot is required for the changes to take effect.
:::
)", 0) \
    DECLARE(Seconds, http_receive_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, R"(
HTTP receive timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).
)", 0) \
    DECLARE(UInt64, http_max_uri_size, 1048576, R"(
Sets the maximum URI length of an HTTP request.

Possible values:

- Positive integer.
)", 0) \
    DECLARE(UInt64, http_max_fields, 1000000, R"(
Maximum number of fields in HTTP header
)", 0) \
    DECLARE(UInt64, http_max_field_name_size, 128 * 1024, R"(
Maximum length of field name in HTTP header
)", 0) \
    DECLARE(UInt64, http_max_field_value_size, 128 * 1024, R"(
Maximum length of field value in HTTP header
)", 0) \
    DECLARE(Bool, http_skip_not_found_url_for_globs, true, R"(
Skip URLs for globs with HTTP_NOT_FOUND error
)", 0) \
    DECLARE(Bool, http_make_head_request, true, R"(
The `http_make_head_request` setting allows the execution of a `HEAD` request while reading data from HTTP to retrieve information about the file to be read, such as its size. Since it's enabled by default, it may be desirable to disable this setting in cases where the server does not support `HEAD` requests.
)", 0) \
    DECLARE(Bool, optimize_throw_if_noop, false, R"(
Enables or disables throwing an exception if an [OPTIMIZE](../../sql-reference/statements/optimize.md) query didn’t perform a merge.

By default, `OPTIMIZE` returns successfully even if it didn’t do anything. This setting lets you differentiate these situations and get the reason in an exception message.

Possible values:

- 1 — Throwing an exception is enabled.
- 0 — Throwing an exception is disabled.
)", 0) \
    DECLARE(Bool, use_index_for_in_with_subqueries, true, R"(
Try using an index if there is a subquery or a table expression on the right side of the IN operator.
)", 0) \
    DECLARE(UInt64, use_index_for_in_with_subqueries_max_values, 0, R"(
The maximum size of the set in the right-hand side of the IN operator to use table index for filtering. It allows to avoid performance degradation and higher memory usage due to the preparation of additional data structures for large queries. Zero means no limit.
)", 0) \
    DECLARE(Bool, analyze_index_with_space_filling_curves, true, R"(
If a table has a space-filling curve in its index, e.g. `ORDER BY mortonEncode(x, y)` or `ORDER BY hilbertEncode(x, y)`, and the query has conditions on its arguments, e.g. `x >= 10 AND x <= 20 AND y >= 20 AND y <= 30`, use the space-filling curve for index analysis.
)", 0) \
    DECLARE(Bool, joined_subquery_requires_alias, true, R"(
Force joined subqueries and table functions to have aliases for correct name qualification.
)", 0) \
    DECLARE(Bool, empty_result_for_aggregation_by_empty_set, false, R"(
Return empty result when aggregating without keys on empty set.
)", 0) \
    DECLARE(Bool, empty_result_for_aggregation_by_constant_keys_on_empty_set, true, R"(
Return empty result when aggregating by constant keys on empty set.
)", 0) \
    DECLARE(Bool, allow_distributed_ddl, true, R"(
If it is set to true, then a user is allowed to executed distributed DDL queries.
)", 0) \
    DECLARE(Bool, allow_suspicious_codecs, false, R"(
If it is set to true, allow to specify meaningless compression codecs.
)", 0) \
    DECLARE(Bool, enable_zstd_qat_codec, false, R"(
If turned on, the ZSTD_QAT codec may be used to compress columns.
)", 0) \
    DECLARE(UInt64, query_profiler_real_time_period_ns, QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS, R"(
Sets the period for a real clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Real clock timer counts wall-clock time.

Possible values:

- Positive integer number, in nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

- 0 for turning off the timer.

**Temporarily disabled in ClickHouse Cloud.**

See also:

- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)
)", 0) \
    DECLARE(UInt64, query_profiler_cpu_time_period_ns, QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS, R"(
Sets the period for a CPU clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). This timer counts only CPU time.

Possible values:

- A positive integer number of nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

- 0 for turning off the timer.

**Temporarily disabled in ClickHouse Cloud.**

See also:

- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)
)", 0) \
    DECLARE(Bool, metrics_perf_events_enabled, false, R"(
If enabled, some of the perf events will be measured throughout queries' execution.
)", 0) \
    DECLARE(String, metrics_perf_events_list, "", R"(
Comma separated list of perf metrics that will be measured throughout queries' execution. Empty means all events. See PerfEventInfo in sources for the available events.
)", 0) \
    DECLARE(Float, opentelemetry_start_trace_probability, 0., R"(
Sets the probability that the ClickHouse can start a trace for executed queries (if no parent [trace context](https://www.w3.org/TR/trace-context/) is supplied).

Possible values:

- 0 — The trace for all executed queries is disabled (if no parent trace context is supplied).
- Positive floating-point number in the range [0..1]. For example, if the setting value is `0,5`, ClickHouse can start a trace on average for half of the queries.
- 1 — The trace for all executed queries is enabled.
)", 0) \
    DECLARE(Bool, opentelemetry_trace_processors, false, R"(
Collect OpenTelemetry spans for processors.
)", 0) \
    DECLARE(Bool, prefer_column_name_to_alias, false, R"(
Enables or disables using the original column names instead of aliases in query expressions and clauses. It especially matters when alias is the same as the column name, see [Expression Aliases](../../sql-reference/syntax.md/#notes-on-usage). Enable this setting to make aliases syntax rules in ClickHouse more compatible with most other database engines.

Possible values:

- 0 — The column name is substituted with the alias.
- 1 — The column name is not substituted with the alias.

**Example**

The difference between enabled and disabled:

Query:

```sql
SET prefer_column_name_to_alias = 0;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Result:

```text
Received exception from server (version 21.5.1):
Code: 184. DB::Exception: Received from localhost:9000. DB::Exception: Aggregate function avg(number) is found inside another aggregate function in query: While processing avg(number) AS number.
```

Query:

```sql
SET prefer_column_name_to_alias = 1;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Result:

```text
┌─number─┬─max(number)─┐
│    4.5 │           9 │
└────────┴─────────────┘
```
)", 0) \
    \
    DECLARE(Bool, prefer_global_in_and_join, false, R"(
Enables the replacement of `IN`/`JOIN` operators with `GLOBAL IN`/`GLOBAL JOIN`.

Possible values:

- 0 — Disabled. `IN`/`JOIN` operators are not replaced with `GLOBAL IN`/`GLOBAL JOIN`.
- 1 — Enabled. `IN`/`JOIN` operators are replaced with `GLOBAL IN`/`GLOBAL JOIN`.

**Usage**

Although `SET distributed_product_mode=global` can change the queries behavior for the distributed tables, it's not suitable for local tables or tables from external resources. Here is when the `prefer_global_in_and_join` setting comes into play.

For example, we have query serving nodes that contain local tables, which are not suitable for distribution. We need to scatter their data on the fly during distributed processing with the `GLOBAL` keyword — `GLOBAL IN`/`GLOBAL JOIN`.

Another use case of `prefer_global_in_and_join` is accessing tables created by external engines. This setting helps to reduce the number of calls to external sources while joining such tables: only one call per query.

**See also:**

- [Distributed subqueries](../../sql-reference/operators/in.md/#select-distributed-subqueries) for more information on how to use `GLOBAL IN`/`GLOBAL JOIN`
)", 0) \
    DECLARE(Bool, enable_vertical_final, true, R"(
If enable, remove duplicated rows during FINAL by marking rows as deleted and filtering them later instead of merging rows
)", 0) \
    \
    \
    /** Limits during query execution are part of the settings. \
      * Used to provide a more safe execution of queries from the user interface. \
      * Basically, limits are checked for each block (not every row). That is, the limits can be slightly violated. \
      * Almost all limits apply only to SELECTs. \
      * Almost all limits apply to each stream individually. \
      */ \
    \
    DECLARE(UInt64, max_rows_to_read, 0, R"(
Limit on read rows from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.
)", 0) \
    DECLARE(UInt64, max_bytes_to_read, 0, R"(
Limit on read bytes (after decompression) from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.
)", 0) \
    DECLARE(OverflowMode, read_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    \
    DECLARE(UInt64, max_rows_to_read_leaf, 0, R"(
Limit on read rows on the leaf nodes for distributed queries. Limit is applied for local reads only, excluding the final merge stage on the root node. Note, the setting is unstable with prefer_localhost_replica=1.
)", 0) \
    DECLARE(UInt64, max_bytes_to_read_leaf, 0, R"(
Limit on read bytes (after decompression) on the leaf nodes for distributed queries. Limit is applied for local reads only, excluding the final merge stage on the root node. Note, the setting is unstable with prefer_localhost_replica=1.
)", 0) \
    DECLARE(OverflowMode, read_overflow_mode_leaf, OverflowMode::THROW, R"(
What to do when the leaf limit is exceeded.
)", 0) \
    \
    DECLARE(UInt64, max_rows_to_group_by, 0, R"(
If aggregation during GROUP BY is generating more than the specified number of rows (unique GROUP BY keys), the behavior will be determined by the 'group_by_overflow_mode' which by default is - throw an exception, but can be also switched to an approximate GROUP BY mode.
)", 0) \
    DECLARE(OverflowModeGroupBy, group_by_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    DECLARE(UInt64, max_bytes_before_external_group_by, 0, R"(
If memory usage during GROUP BY operation is exceeding this threshold in bytes, activate the 'external aggregation' mode (spill data to disk). Recommended value is half of the available system memory.
)", 0) \
    \
    DECLARE(UInt64, max_rows_to_sort, 0, R"(
If more than the specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception
)", 0) \
    DECLARE(UInt64, max_bytes_to_sort, 0, R"(
If more than the specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception
)", 0) \
    DECLARE(OverflowMode, sort_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    DECLARE(UInt64, prefer_external_sort_block_bytes, DEFAULT_BLOCK_SIZE * 256, R"(
Prefer maximum block bytes for external sort, reduce the memory usage during merging.
)", 0) \
    DECLARE(UInt64, max_bytes_before_external_sort, 0, R"(
If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the 'external sorting' mode (spill data to disk). Recommended value is half of the available system memory.
)", 0) \
    DECLARE(UInt64, max_bytes_before_remerge_sort, 1000000000, R"(
In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.
)", 0) \
    DECLARE(Float, remerge_sort_lowered_memory_bytes_ratio, 2., R"(
If memory usage after remerge does not reduced by this ratio, remerge will be disabled.
)", 0) \
    \
    DECLARE(UInt64, max_result_rows, 0, R"(
Limit on result size in rows. The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold.
)", 0) \
    DECLARE(UInt64, max_result_bytes, 0, R"(
Limit on result size in bytes (uncompressed).  The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold. Caveats: the result size in memory is taken into account for this threshold. Even if the result size is small, it can reference larger data structures in memory, representing dictionaries of LowCardinality columns, and Arenas of AggregateFunction columns, so the threshold can be exceeded despite the small result size. The setting is fairly low level and should be used with caution.
)", 0) \
    DECLARE(OverflowMode, result_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    \
    /* TODO: Check also when merging and finalizing aggregate functions. */ \
    DECLARE(Seconds, max_execution_time, 0, R"(
If query runtime exceeds the specified number of seconds, the behavior will be determined by the 'timeout_overflow_mode', which by default is - throw an exception. Note that the timeout is checked and the query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting.
)", 0) \
    DECLARE(OverflowMode, timeout_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    DECLARE(Seconds, max_execution_time_leaf, 0, R"(
Similar semantic to max_execution_time but only apply on leaf node for distributed queries, the time out behavior will be determined by 'timeout_overflow_mode_leaf' which by default is - throw an exception
)", 0) \
    DECLARE(OverflowMode, timeout_overflow_mode_leaf, OverflowMode::THROW, R"(
What to do when the leaf limit is exceeded.
)", 0) \
    \
    DECLARE(UInt64, min_execution_speed, 0, R"(
Minimum number of execution rows per second.
)", 0) \
    DECLARE(UInt64, max_execution_speed, 0, R"(
Maximum number of execution rows per second.
)", 0) \
    DECLARE(UInt64, min_execution_speed_bytes, 0, R"(
Minimum number of execution bytes per second.
)", 0) \
    DECLARE(UInt64, max_execution_speed_bytes, 0, R"(
Maximum number of execution bytes per second.
)", 0) \
    DECLARE(Seconds, timeout_before_checking_execution_speed, 10, R"(
Check that the speed is not too low after the specified time has elapsed.
)", 0) \
    DECLARE(Seconds, max_estimated_execution_time, 0, R"(
Maximum query estimate execution time in seconds.
)", 0) \
    \
    DECLARE(UInt64, max_columns_to_read, 0, R"(
If a query requires reading more than specified number of columns, exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.
)", 0) \
    DECLARE(UInt64, max_temporary_columns, 0, R"(
If a query generates more than the specified number of temporary columns in memory as a result of intermediate calculation, the exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.
)", 0) \
    DECLARE(UInt64, max_temporary_non_const_columns, 0, R"(
Similar to the 'max_temporary_columns' setting but applies only to non-constant columns. This makes sense because constant columns are cheap and it is reasonable to allow more of them.
)", 0) \
    \
    DECLARE(UInt64, max_sessions_for_user, 0, R"(
Maximum number of simultaneous sessions for a user.
)", 0) \
    \
    DECLARE(UInt64, max_subquery_depth, 100, R"(
If a query has more than the specified number of nested subqueries, throw an exception. This allows you to have a sanity check to protect the users of your cluster from going insane with their queries.
)", 0) \
    DECLARE(UInt64, max_analyze_depth, 5000, R"(
Maximum number of analyses performed by interpreter.
)", 0) \
    DECLARE(UInt64, max_ast_depth, 1000, R"(
Maximum depth of query syntax tree. Checked after parsing.
)", 0) \
    DECLARE(UInt64, max_ast_elements, 50000, R"(
Maximum size of query syntax tree in number of nodes. Checked after parsing.
)", 0) \
    DECLARE(UInt64, max_expanded_ast_elements, 500000, R"(
Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.
)", 0) \
    \
    DECLARE(UInt64, readonly, 0, R"(
0 - no read-only restrictions. 1 - only read requests, as well as changing explicitly allowed settings. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.
)", 0) \
    \
    DECLARE(UInt64, max_rows_in_set, 0, R"(
Maximum size of the set (in number of elements) resulting from the execution of the IN section.
)", 0) \
    DECLARE(UInt64, max_bytes_in_set, 0, R"(
Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.
)", 0) \
    DECLARE(OverflowMode, set_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    \
    DECLARE(UInt64, max_rows_in_join, 0, R"(
Maximum size of the hash table for JOIN (in number of rows).
)", 0) \
    DECLARE(UInt64, max_bytes_in_join, 0, R"(
Maximum size of the hash table for JOIN (in number of bytes in memory).
)", 0) \
    DECLARE(OverflowMode, join_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    DECLARE(Bool, join_any_take_last_row, false, R"(
Changes the behaviour of join operations with `ANY` strictness.

:::note
This setting applies only for `JOIN` operations with [Join](../../engines/table-engines/special/join.md) engine tables.
:::

Possible values:

- 0 — If the right table has more than one matching row, only the first one found is joined.
- 1 — If the right table has more than one matching row, only the last one found is joined.

See also:

- [JOIN clause](../../sql-reference/statements/select/join.md/#select-join)
- [Join table engine](../../engines/table-engines/special/join.md)
- [join_default_strictness](#join_default_strictness)
)", IMPORTANT) \
    DECLARE(JoinAlgorithm, join_algorithm, JoinAlgorithm::DEFAULT, R"(
Specifies which [JOIN](../../sql-reference/statements/select/join.md) algorithm is used.

Several algorithms can be specified, and an available one would be chosen for a particular query based on kind/strictness and table engine.

Possible values:

- default

 This is the equivalent of `hash` or `direct`, if possible (same as `direct,hash`)

- grace_hash

 [Grace hash join](https://en.wikipedia.org/wiki/Hash_join#Grace_hash_join) is used.  Grace hash provides an algorithm option that provides performant complex joins while limiting memory use.

 The first phase of a grace join reads the right table and splits it into N buckets depending on the hash value of key columns (initially, N is `grace_hash_join_initial_buckets`). This is done in a way to ensure that each bucket can be processed independently. Rows from the first bucket are added to an in-memory hash table while the others are saved to disk. If the hash table grows beyond the memory limit (e.g., as set by [`max_bytes_in_join`](/docs/en/operations/settings/query-complexity.md/#max_bytes_in_join)), the number of buckets is increased and the assigned bucket for each row. Any rows which don’t belong to the current bucket are flushed and reassigned.

 Supports `INNER/LEFT/RIGHT/FULL ALL/ANY JOIN`.

- hash

 [Hash join algorithm](https://en.wikipedia.org/wiki/Hash_join) is used. The most generic implementation that supports all combinations of kind and strictness and multiple join keys that are combined with `OR` in the `JOIN ON` section.

- parallel_hash

 A variation of `hash` join that splits the data into buckets and builds several hashtables instead of one concurrently to speed up this process.

 When using the `hash` algorithm, the right part of `JOIN` is uploaded into RAM.

- partial_merge

 A variation of the [sort-merge algorithm](https://en.wikipedia.org/wiki/Sort-merge_join), where only the right table is fully sorted.

 The `RIGHT JOIN` and `FULL JOIN` are supported only with `ALL` strictness (`SEMI`, `ANTI`, `ANY`, and `ASOF` are not supported).

 When using the `partial_merge` algorithm, ClickHouse sorts the data and dumps it to the disk. The `partial_merge` algorithm in ClickHouse differs slightly from the classic realization. First, ClickHouse sorts the right table by joining keys in blocks and creates a min-max index for sorted blocks. Then it sorts parts of the left table by the `join key` and joins them over the right table. The min-max index is also used to skip unneeded right table blocks.

- direct

 This algorithm can be applied when the storage for the right table supports key-value requests.

 The `direct` algorithm performs a lookup in the right table using rows from the left table as keys. It's supported only by special storage such as [Dictionary](../../engines/table-engines/special/dictionary.md/#dictionary) or [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) and only the `LEFT` and `INNER` JOINs.

- auto

 When set to `auto`, `hash` join is tried first, and the algorithm is switched on the fly to another algorithm if the memory limit is violated.

- full_sorting_merge

 [Sort-merge algorithm](https://en.wikipedia.org/wiki/Sort-merge_join) with full sorting joined tables before joining.

- prefer_partial_merge

 ClickHouse always tries to use `partial_merge` join if possible, otherwise, it uses `hash`. *Deprecated*, same as `partial_merge,hash`.
)", 0) \
    DECLARE(UInt64, cross_join_min_rows_to_compress, 10000000, R"(
Minimal count of rows to compress block in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.
)", 0) \
    DECLARE(UInt64, cross_join_min_bytes_to_compress, 1_GiB, R"(
Minimal size of block to compress in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.
)", 0) \
    DECLARE(UInt64, default_max_bytes_in_join, 1000000000, R"(
Maximum size of right-side table if limit is required but max_bytes_in_join is not set.
)", 0) \
    DECLARE(UInt64, partial_merge_join_left_table_buffer_bytes, 0, R"(
If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread.
)", 0) \
    DECLARE(UInt64, partial_merge_join_rows_in_right_blocks, 65536, R"(
Limits sizes of right-hand join data blocks in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.

ClickHouse server:

1.  Splits right-hand join data into blocks with up to the specified number of rows.
2.  Indexes each block with its minimum and maximum values.
3.  Unloads prepared blocks to disk if it is possible.

Possible values:

- Any positive integer. Recommended range of values: \[1000, 100000\].
)", 0) \
    DECLARE(UInt64, join_on_disk_max_files_to_merge, 64, R"(
Limits the number of files allowed for parallel sorting in MergeJoin operations when they are executed on disk.

The bigger the value of the setting, the more RAM is used and the less disk I/O is needed.

Possible values:

- Any positive integer, starting from 2.
)", 0) \
    DECLARE(UInt64, max_rows_in_set_to_optimize_join, 0, R"(
Maximal size of the set to filter joined tables by each other's row sets before joining.

Possible values:

- 0 — Disable.
- Any positive integer.
)", 0) \
    \
    DECLARE(Bool, compatibility_ignore_collation_in_create_table, true, R"(
Compatibility ignore collation in create table
)", 0) \
    \
    DECLARE(String, temporary_files_codec, "LZ4", R"(
Sets compression codec for temporary files used in sorting and joining operations on disk.

Possible values:

- LZ4 — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression is applied.
- NONE — No compression is applied.
)", 0) \
    \
    DECLARE(UInt64, max_rows_to_transfer, 0, R"(
Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.
)", 0) \
    DECLARE(UInt64, max_bytes_to_transfer, 0, R"(
Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.
)", 0) \
    DECLARE(OverflowMode, transfer_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    \
    DECLARE(UInt64, max_rows_in_distinct, 0, R"(
Maximum number of elements during execution of DISTINCT.
)", 0) \
    DECLARE(UInt64, max_bytes_in_distinct, 0, R"(
Maximum total size of the state (in uncompressed bytes) in memory for the execution of DISTINCT.
)", 0) \
    DECLARE(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, R"(
What to do when the limit is exceeded.
)", 0) \
    \
    DECLARE(UInt64, max_memory_usage, 0, R"(
Maximum memory usage for processing of single query. Zero means unlimited.
)", 0) \
    DECLARE(UInt64, memory_overcommit_ratio_denominator, 1_GiB, R"(
It represents the soft memory limit when the hard limit is reached on the global level.
This value is used to compute the overcommit ratio for the query.
Zero means skip the query.
Read more about [memory overcommit](memory-overcommit.md).
)", 0) \
    DECLARE(UInt64, max_memory_usage_for_user, 0, R"(
Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.
)", 0) \
    DECLARE(UInt64, memory_overcommit_ratio_denominator_for_user, 1_GiB, R"(
It represents the soft memory limit when the hard limit is reached on the user level.
This value is used to compute the overcommit ratio for the query.
Zero means skip the query.
Read more about [memory overcommit](memory-overcommit.md).
)", 0) \
    DECLARE(UInt64, max_untracked_memory, (4 * 1024 * 1024), R"(
Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when an amount (in absolute value) becomes larger than the specified value. If the value is higher than 'memory_profiler_step' it will be effectively lowered to 'memory_profiler_step'.
)", 0) \
    DECLARE(UInt64, memory_profiler_step, (4 * 1024 * 1024), R"(
Sets the step of memory profiler. Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stacktrace and will write it into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- A positive integer number of bytes.

- 0 for turning off the memory profiler.
)", 0) \
    DECLARE(Float, memory_profiler_sample_probability, 0., R"(
Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability is for every alloc/free regardless of the size of the allocation (can be changed with `memory_profiler_sample_min_allocation_size` and `memory_profiler_sample_max_allocation_size`). Note that sampling happens only when the amount of untracked memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine-grained sampling.
)", 0) \
    DECLARE(UInt64, memory_profiler_sample_min_allocation_size, 0, R"(
Collect random allocations of size greater or equal than the specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold work as expected.
)", 0) \
    DECLARE(UInt64, memory_profiler_sample_max_allocation_size, 0, R"(
Collect random allocations of size less or equal than the specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold work as expected.
)", 0) \
    DECLARE(Bool, trace_profile_events, false, R"(
Enables or disables collecting stacktraces on each update of profile events along with the name of profile event and the value of increment and sending them into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- 1 — Tracing of profile events enabled.
- 0 — Tracing of profile events disabled.
)", 0) \
    \
    DECLARE(UInt64, memory_usage_overcommit_max_wait_microseconds, 5'000'000, R"(
Maximum time thread will wait for memory to be freed in the case of memory overcommit on a user level.
If the timeout is reached and memory is not freed, an exception is thrown.
Read more about [memory overcommit](memory-overcommit.md).
)", 0) \
    \
    DECLARE(UInt64, max_network_bandwidth, 0, R"(
Limits the speed of the data exchange over the network in bytes per second. This setting applies to every query.

Possible values:

- Positive integer.
- 0 — Bandwidth control is disabled.
)", 0) \
    DECLARE(UInt64, max_network_bytes, 0, R"(
Limits the data volume (in bytes) that is received or transmitted over the network when executing a query. This setting applies to every individual query.

Possible values:

- Positive integer.
- 0 — Data volume control is disabled.
)", 0) \
    DECLARE(UInt64, max_network_bandwidth_for_user, 0, R"(
Limits the speed of the data exchange over the network in bytes per second. This setting applies to all concurrently running queries performed by a single user.

Possible values:

- Positive integer.
- 0 — Control of the data speed is disabled.
)", 0)\
    DECLARE(UInt64, max_network_bandwidth_for_all_users, 0, R"(
Limits the speed that data is exchanged at over the network in bytes per second. This setting applies to all concurrently running queries on the server.

Possible values:

- Positive integer.
- 0 — Control of the data speed is disabled.
)", 0) \
    \
    DECLARE(UInt64, max_temporary_data_on_disk_size_for_user, 0, R"(
The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries. Zero means unlimited.
)", 0)\
    DECLARE(UInt64, max_temporary_data_on_disk_size_for_query, 0, R"(
The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries. Zero means unlimited.
)", 0)\
    \
    DECLARE(UInt64, backup_restore_keeper_max_retries, 1000, R"(
Max retries for [Zoo]Keeper operations in the middle of a BACKUP or RESTORE operation.
Should be big enough so the whole operation won't fail because of a temporary [Zoo]Keeper failure.
)", 0) \
    DECLARE(UInt64, backup_restore_keeper_retry_initial_backoff_ms, 100, R"(
Initial backoff timeout for [Zoo]Keeper operations during backup or restore
)", 0) \
    DECLARE(UInt64, backup_restore_keeper_retry_max_backoff_ms, 5000, R"(
Max backoff timeout for [Zoo]Keeper operations during backup or restore
)", 0) \
    DECLARE(UInt64, backup_restore_failure_after_host_disconnected_for_seconds, 3600, R"(
If a host during a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation doesn't recreate its ephemeral 'alive' node in ZooKeeper for this amount of time then the whole backup or restore is considered as failed.
This value should be bigger than any reasonable time for a host to reconnect to ZooKeeper after a failure.
Zero means unlimited.
)", 0) \
    DECLARE(UInt64, backup_restore_keeper_max_retries_while_initializing, 20, R"(
Max retries for [Zoo]Keeper operations during the initialization of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
)", 0) \
    DECLARE(UInt64, backup_restore_keeper_max_retries_while_handling_error, 20, R"(
Max retries for [Zoo]Keeper operations while handling an error of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
)", 0) \
    DECLARE(UInt64, backup_restore_finish_timeout_after_error_sec, 180, R"(
How long the initiator should wait for other host to react to the 'error' node and stop their work on the current BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
)", 0) \
    DECLARE(UInt64, backup_restore_keeper_value_max_size, 1048576, R"(
Maximum size of data of a [Zoo]Keeper's node during backup
)", 0) \
    DECLARE(UInt64, backup_restore_batch_size_for_keeper_multi, 1000, R"(
Maximum size of batch for multi request to [Zoo]Keeper during backup or restore
)", 0) \
    DECLARE(UInt64, backup_restore_batch_size_for_keeper_multiread, 10000, R"(
Maximum size of batch for multiread request to [Zoo]Keeper during backup or restore
)", 0) \
    DECLARE(Float, backup_restore_keeper_fault_injection_probability, 0.0f, R"(
Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f]
)", 0) \
    DECLARE(UInt64, backup_restore_keeper_fault_injection_seed, 0, R"(
0 - random seed, otherwise the setting value
)", 0) \
    DECLARE(UInt64, backup_restore_s3_retry_attempts, 1000, R"(
Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries. It takes place only for backup/restore.
)", 0) \
    DECLARE(UInt64, max_backup_bandwidth, 0, R"(
The maximum read speed in bytes per second for particular backup on server. Zero means unlimited.
)", 0) \
    \
    DECLARE(Bool, log_profile_events, true, R"(
Log query performance statistics into the query_log, query_thread_log and query_views_log.
)", 0) \
    DECLARE(Bool, log_query_settings, true, R"(
Log query settings into the query_log and OpenTelemetry span log.
)", 0) \
    DECLARE(Bool, log_query_threads, false, R"(
Setting up query threads logging.

Query threads log into the [system.query_thread_log](../../operations/system-tables/query_thread_log.md) table. This setting has effect only when [log_queries](#log-queries) is true. Queries’ threads run by ClickHouse with this setup are logged according to the rules in the [query_thread_log](../../operations/server-configuration-parameters/settings.md/#query_thread_log) server configuration parameter.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

``` text
log_query_threads=1
```
)", 0) \
    DECLARE(Bool, log_query_views, true, R"(
Setting up query views logging.

When a query run by ClickHouse with this setting enabled has associated views (materialized or live views), they are logged in the [query_views_log](../../operations/server-configuration-parameters/settings.md/#query_views_log) server configuration parameter.

Example:

``` text
log_query_views=1
```
)", 0) \
    DECLARE(String, log_comment, "", R"(
Specifies the value for the `log_comment` field of the [system.query_log](../system-tables/query_log.md) table and comment text for the server log.

It can be used to improve the readability of server logs. Additionally, it helps to select queries related to the test from the `system.query_log` after running [clickhouse-test](../../development/tests.md).

Possible values:

- Any string no longer than [max_query_size](#max_query_size). If the max_query_size is exceeded, the server throws an exception.

**Example**

Query:

``` sql
SET log_comment = 'log_comment test', log_queries = 1;
SELECT 1;
SYSTEM FLUSH LOGS;
SELECT type, query FROM system.query_log WHERE log_comment = 'log_comment test' AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 2;
```

Result:

``` text
┌─type────────┬─query─────┐
│ QueryStart  │ SELECT 1; │
│ QueryFinish │ SELECT 1; │
└─────────────┴───────────┘
```
)", 0) \
    DECLARE(Int64, query_metric_log_interval, -1, R"(
The interval in milliseconds at which the [query_metric_log](../../operations/system-tables/query_metric_log.md) for individual queries is collected.

If set to any negative value, it will take the value `collect_interval_milliseconds` from the [query_metric_log setting](../../operations/server-configuration-parameters/settings.md#query_metric_log) or default to 1000 if not present.

To disable the collection of a single query, set `query_metric_log_interval` to 0.

Default value: -1
    )", 0) \
    DECLARE(LogsLevel, send_logs_level, LogsLevel::fatal, R"(
Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'
)", 0) \
    DECLARE(String, send_logs_source_regexp, "", R"(
Send server text logs with specified regexp to match log source name. Empty means all sources.
)", 0) \
    DECLARE(Bool, enable_optimize_predicate_expression, true, R"(
Turns on predicate pushdown in `SELECT` queries.

Predicate pushdown may significantly reduce network traffic for distributed queries.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Usage

Consider the following queries:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

If `enable_optimize_predicate_expression = 1`, then the execution time of these queries is equal because ClickHouse applies `WHERE` to the subquery when processing it.

If `enable_optimize_predicate_expression = 0`, then the execution time of the second query is much longer because the `WHERE` clause applies to all the data after the subquery finishes.
)", 0) \
    DECLARE(Bool, enable_optimize_predicate_expression_to_final_subquery, true, R"(
Allow push predicate to final subquery.
)", 0) \
    DECLARE(Bool, allow_push_predicate_when_subquery_contains_with, true, R"(
Allows push predicate when subquery contains WITH clause
)", 0) \
    \
    DECLARE(UInt64, low_cardinality_max_dictionary_size, 8192, R"(
Sets a maximum size in rows of a shared global dictionary for the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type that can be written to a storage file system. This setting prevents issues with RAM in case of unlimited dictionary growth. All the data that can’t be encoded due to maximum dictionary size limitation ClickHouse writes in an ordinary method.

Possible values:

- Any positive integer.
)", 0) \
    DECLARE(Bool, low_cardinality_use_single_dictionary_for_part, false, R"(
Turns on or turns off using of single dictionary for the data part.

By default, the ClickHouse server monitors the size of dictionaries and if a dictionary overflows then the server starts to write the next one. To prohibit creating several dictionaries set `low_cardinality_use_single_dictionary_for_part = 1`.

Possible values:

- 1 — Creating several dictionaries for the data part is prohibited.
- 0 — Creating several dictionaries for the data part is not prohibited.
)", 0) \
    DECLARE(Bool, decimal_check_overflow, true, R"(
Check overflow of decimal arithmetic/comparison operations
)", 0) \
    DECLARE(Bool, allow_custom_error_code_in_throwif, false, R"(
Enable custom error code in function throwIf(). If true, thrown exceptions may have unexpected error codes.
)", 0) \
    \
    DECLARE(Bool, prefer_localhost_replica, true, R"(
Enables/disables preferable using the localhost replica when processing distributed queries.

Possible values:

- 1 — ClickHouse always sends a query to the localhost replica if it exists.
- 0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#load_balancing) setting.

:::note
Disable this setting if you use [max_parallel_replicas](#max_parallel_replicas) without [parallel_replicas_custom_key](#parallel_replicas_custom_key).
If [parallel_replicas_custom_key](#parallel_replicas_custom_key) is set, disable this setting only if it's used on a cluster with multiple shards containing multiple replicas.
If it's used on a cluster with a single shard and multiple replicas, disabling this setting will have negative effects.
:::
)", 0) \
    DECLARE(UInt64, max_fetch_partition_retries_count, 5, R"(
Amount of retries while fetching partition from another host.
)", 0) \
    DECLARE(UInt64, http_max_multipart_form_data_size, 1024 * 1024 * 1024, R"(
Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in a user profile. Note that content is parsed and external tables are created in memory before the start of query execution. And this is the only limit that has an effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).
)", 0) \
    DECLARE(Bool, calculate_text_stack_trace, true, R"(
Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when a huge amount of wrong queries are executed. In normal cases, you should not disable this option.
)", 0) \
    DECLARE(Bool, enable_job_stack_trace, false, R"(
Output stack trace of a job creator when job results in exception
)", 0) \
    DECLARE(Bool, allow_ddl, true, R"(
If it is set to true, then a user is allowed to executed DDL queries.
)", 0) \
    DECLARE(Bool, parallel_view_processing, false, R"(
Enables pushing to attached views concurrently instead of sequentially.
)", 0) \
    DECLARE(Bool, enable_unaligned_array_join, false, R"(
Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.
)", 0) \
    DECLARE(Bool, optimize_read_in_order, true, R"(
Enables [ORDER BY](../../sql-reference/statements/select/order-by.md/#optimize_read_in_order) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for reading data from [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

- 0 — `ORDER BY` optimization is disabled.
- 1 — `ORDER BY` optimization is enabled.

**See Also**

- [ORDER BY Clause](../../sql-reference/statements/select/order-by.md/#optimize_read_in_order)
)", 0) \
    DECLARE(Bool, optimize_read_in_window_order, true, R"(
Enable ORDER BY optimization in window clause for reading data in corresponding order in MergeTree tables.
)", 0) \
    DECLARE(Bool, optimize_aggregation_in_order, false, R"(
Enables [GROUP BY](../../sql-reference/statements/select/group-by.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for aggregating data in corresponding order in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

- 0 — `GROUP BY` optimization is disabled.
- 1 — `GROUP BY` optimization is enabled.

**See Also**

- [GROUP BY optimization](../../sql-reference/statements/select/group-by.md/#aggregation-in-order)
)", 0) \
    DECLARE(Bool, read_in_order_use_buffering, true, R"(
Use buffering before merging while reading in order of primary key. It increases the parallelism of query execution
)", 0) \
    DECLARE(UInt64, aggregation_in_order_max_block_bytes, 50000000, R"(
Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.
)", 0) \
    DECLARE(UInt64, read_in_order_two_level_merge_threshold, 100, R"(
Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.
)", 0) \
    DECLARE(Bool, low_cardinality_allow_in_native_format, true, R"(
Allows or restricts using the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type with the [Native](../../interfaces/formats.md/#native) format.

If usage of `LowCardinality` is restricted, ClickHouse server converts `LowCardinality`-columns to ordinary ones for `SELECT` queries, and convert ordinary columns to `LowCardinality`-columns for `INSERT` queries.

This setting is required mainly for third-party clients which do not support `LowCardinality` data type.

Possible values:

- 1 — Usage of `LowCardinality` is not restricted.
- 0 — Usage of `LowCardinality` is restricted.
)", 0) \
    DECLARE(Bool, cancel_http_readonly_queries_on_client_close, false, R"(
Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Cloud default value: `1`.
)", 0) \
    DECLARE(Bool, external_table_functions_use_nulls, true, R"(
Defines how [mysql](../../sql-reference/table-functions/mysql.md), [postgresql](../../sql-reference/table-functions/postgresql.md) and [odbc](../../sql-reference/table-functions/odbc.md) table functions use Nullable columns.

Possible values:

- 0 — The table function explicitly uses Nullable columns.
- 1 — The table function implicitly uses Nullable columns.

**Usage**

If the setting is set to `0`, the table function does not make Nullable columns and inserts default values instead of NULL. This is also applicable for NULL values inside arrays.
)", 0) \
    DECLARE(Bool, external_table_strict_query, false, R"(
If it is set to true, transforming expression to local filter is forbidden for queries to external tables.
)", 0) \
    \
    DECLARE(Bool, allow_hyperscan, true, R"(
Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.
)", 0) \
    DECLARE(UInt64, max_hyperscan_regexp_length, 0, R"(
Defines the maximum length for each regular expression in the [hyperscan multi-match functions](../../sql-reference/functions/string-search-functions.md/#multimatchanyhaystack-pattern1-pattern2-patternn).

Possible values:

- Positive integer.
- 0 - The length is not limited.

**Example**

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 3;
```

Result:

```text
┌─multiMatchAny('abcd', ['ab', 'bcd', 'c', 'd'])─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 2;
```

Result:

```text
Exception: Regexp length too large.
```

**See Also**

- [max_hyperscan_regexp_total_length](#max-hyperscan-regexp-total-length)
)", 0) \
    DECLARE(UInt64, max_hyperscan_regexp_total_length, 0, R"(
Sets the maximum length total of all regular expressions in each [hyperscan multi-match function](../../sql-reference/functions/string-search-functions.md/#multimatchanyhaystack-pattern1-pattern2-patternn).

Possible values:

- Positive integer.
- 0 - The length is not limited.

**Example**

Query:

```sql
SELECT multiMatchAny('abcd', ['a','b','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Result:

```text
┌─multiMatchAny('abcd', ['a', 'b', 'c', 'd'])─┐
│                                           1 │
└─────────────────────────────────────────────┘
```

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bc','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Result:

```text
Exception: Total regexp lengths too large.
```

**See Also**

- [max_hyperscan_regexp_length](#max-hyperscan-regexp-length)
)", 0) \
    DECLARE(Bool, reject_expensive_hyperscan_regexps, true, R"(
Reject patterns which will likely be expensive to evaluate with hyperscan (due to NFA state explosion)
)", 0) \
    DECLARE(Bool, allow_simdjson, true, R"(
Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.
)", 0) \
    DECLARE(Bool, allow_introspection_functions, false, R"(
Enables or disables [introspection functions](../../sql-reference/functions/introspection.md) for query profiling.

Possible values:

- 1 — Introspection functions enabled.
- 0 — Introspection functions disabled.

**See Also**

- [Sampling Query Profiler](../../operations/optimizing-performance/sampling-query-profiler.md)
- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)
)", 0) \
    DECLARE(Bool, splitby_max_substrings_includes_remaining_string, false, R"(
Controls whether function [splitBy*()](../../sql-reference/functions/splitting-merging-functions.md) with argument `max_substrings` > 0 will include the remaining string in the last element of the result array.

Possible values:

- `0` - The remaining string will not be included in the last element of the result array.
- `1` - The remaining string will be included in the last element of the result array. This is the behavior of Spark's [`split()`](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.split.html) function and Python's ['string.split()'](https://docs.python.org/3/library/stdtypes.html#str.split) method.
)", 0) \
    \
    DECLARE(Bool, allow_execute_multiif_columnar, true, R"(
Allow execute multiIf function columnar
)", 0) \
    DECLARE(Bool, formatdatetime_f_prints_single_zero, false, R"(
Formatter '%f' in function 'formatDateTime()' prints a single zero instead of six zeros if the formatted value has no fractional seconds.
)", 0) \
    DECLARE(Bool, formatdatetime_parsedatetime_m_is_month_name, true, R"(
Formatter '%M' in functions 'formatDateTime()' and 'parseDateTime()' print/parse the month name instead of minutes.
)", 0) \
    DECLARE(Bool, parsedatetime_parse_without_leading_zeros, true, R"(
Formatters '%c', '%l' and '%k' in function 'parseDateTime()' parse months and hours without leading zeros.
)", 0) \
    DECLARE(Bool, formatdatetime_format_without_leading_zeros, false, R"(
Formatters '%c', '%l' and '%k' in function 'formatDateTime()' print months and hours without leading zeros.
)", 0) \
    \
    DECLARE(UInt64, max_partitions_per_insert_block, 100, R"(
Limit maximum number of partitions in the single INSERTed block. Zero means unlimited. Throw an exception if the block contains too many partitions. This setting is a safety threshold because using a large number of partitions is a common misconception.
)", 0) \
    DECLARE(Bool, throw_on_max_partitions_per_insert_block, true, R"(
Used with max_partitions_per_insert_block. If true (default), an exception will be thrown when max_partitions_per_insert_block is reached. If false, details of the insert query reaching this limit with the number of partitions will be logged. This can be useful if you're trying to understand the impact on users when changing max_partitions_per_insert_block.
)", 0) \
    DECLARE(Int64, max_partitions_to_read, -1, R"(
Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited.
)", 0) \
    DECLARE(Bool, check_query_single_value_result, true, R"(
Defines the level of detail for the [CHECK TABLE](../../sql-reference/statements/check-table.md/#checking-mergetree-tables) query result for `MergeTree` family engines .

Possible values:

- 0 — the query shows a check status for every individual data part of a table.
- 1 — the query shows the general table check status.
)", 0) \
    DECLARE(Bool, allow_drop_detached, false, R"(
Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries
)", 0) \
    DECLARE(UInt64, max_parts_to_move, 1000, "Limit the number of parts that can be moved in one query. Zero means unlimited.", 0) \
    \
    DECLARE(UInt64, max_table_size_to_drop, 50000000000lu, R"(
Restriction on deleting tables in query time. The value 0 means that you can delete all tables without any restrictions.

Cloud default value: 1 TB.

:::note
This query setting overwrites its server setting equivalent, see [max_table_size_to_drop](/docs/en/operations/server-configuration-parameters/settings.md/#max-table-size-to-drop)
:::
)", 0) \
    DECLARE(UInt64, max_partition_size_to_drop, 50000000000lu, R"(
Restriction on dropping partitions in query time. The value 0 means that you can drop partitions without any restrictions.

Cloud default value: 1 TB.

:::note
This query setting overwrites its server setting equivalent, see [max_partition_size_to_drop](/docs/en/operations/server-configuration-parameters/settings.md/#max-partition-size-to-drop)
:::
)", 0) \
    \
    DECLARE(UInt64, postgresql_connection_pool_size, 16, R"(
Connection pool size for PostgreSQL table engine and database engine.
)", 0) \
    DECLARE(UInt64, postgresql_connection_attempt_timeout, 2, R"(
Connection timeout in seconds of a single attempt to connect PostgreSQL end-point.
The value is passed as a `connect_timeout` parameter of the connection URL.
)", 0) \
    DECLARE(UInt64, postgresql_connection_pool_wait_timeout, 5000, R"(
Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty pool.
)", 0) \
    DECLARE(UInt64, postgresql_connection_pool_retries, 2, R"(
Connection pool push/pop retries number for PostgreSQL table engine and database engine.
)", 0) \
    DECLARE(Bool, postgresql_connection_pool_auto_close_connection, false, R"(
Close connection before returning connection to the pool.
)", 0) \
    DECLARE(UInt64, glob_expansion_max_elements, 1000, R"(
Maximum number of allowed addresses (For external storages, table functions, etc).
)", 0) \
    DECLARE(UInt64, odbc_bridge_connection_pool_size, 16, R"(
Connection pool size for each connection settings string in ODBC bridge.
)", 0) \
    DECLARE(Bool, odbc_bridge_use_connection_pooling, true, R"(
Use connection pooling in ODBC bridge. If set to false, a new connection is created every time.
)", 0) \
    \
    DECLARE(Seconds, distributed_replica_error_half_life, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD, R"(
- Type: seconds
- Default value: 60 seconds

Controls how fast errors in distributed tables are zeroed. If a replica is unavailable for some time, accumulates 5 errors, and distributed_replica_error_half_life is set to 1 second, then the replica is considered normal 3 seconds after the last error.

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap](#distributed_replica_error_cap)
- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)
)", 0) \
    DECLARE(UInt64, distributed_replica_error_cap, DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT, R"(
- Type: unsigned int
- Default value: 1000

The error count of each replica is capped at this value, preventing a single replica from accumulating too many errors.

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_half_life](#distributed_replica_error_half_life)
- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)
)", 0) \
    DECLARE(UInt64, distributed_replica_max_ignored_errors, 0, R"(
- Type: unsigned int
- Default value: 0

The number of errors that will be ignored while choosing replicas (according to `load_balancing` algorithm).

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap](#distributed_replica_error_cap)
- [distributed_replica_error_half_life](#distributed_replica_error_half_life)
)", 0) \
    \
    DECLARE(UInt64, min_free_disk_space_for_temporary_data, 0, R"(
The minimum disk space to keep while writing temporary data used in external sorting and aggregation.
)", 0) \
    \
    DECLARE(DefaultTableEngine, default_temporary_table_engine, DefaultTableEngine::Memory, R"(
Same as [default_table_engine](#default_table_engine) but for temporary tables.

In this example, any new temporary table that does not specify an `Engine` will use the `Log` table engine:

Query:

```sql
SET default_temporary_table_engine = 'Log';

CREATE TEMPORARY TABLE my_table (
    x UInt32,
    y UInt32
);

SHOW CREATE TEMPORARY TABLE my_table;
```

Result:

```response
┌─statement────────────────────────────────────────────────────────────────┐
│ CREATE TEMPORARY TABLE default.my_table
(
    `x` UInt32,
    `y` UInt32
)
ENGINE = Log
└──────────────────────────────────────────────────────────────────────────┘
```
)", 0) \
    DECLARE(DefaultTableEngine, default_table_engine, DefaultTableEngine::MergeTree, R"(
Default table engine to use when `ENGINE` is not set in a `CREATE` statement.

Possible values:

- a string representing any valid table engine name

Cloud default value: `SharedMergeTree`.

**Example**

Query:

```sql
SET default_table_engine = 'Log';

SELECT name, value, changed FROM system.settings WHERE name = 'default_table_engine';
```

Result:

```response
┌─name─────────────────┬─value─┬─changed─┐
│ default_table_engine │ Log   │       1 │
└──────────────────────┴───────┴─────────┘
```

In this example, any new table that does not specify an `Engine` will use the `Log` table engine:

Query:

```sql
CREATE TABLE my_table (
    x UInt32,
    y UInt32
);

SHOW CREATE TABLE my_table;
```

Result:

```response
┌─statement────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.my_table
(
    `x` UInt32,
    `y` UInt32
)
ENGINE = Log
└──────────────────────────────────────────────────────────────────────────┘
```
)", 0) \
    DECLARE(Bool, show_table_uuid_in_table_create_query_if_not_nil, false, R"(
Sets the `SHOW TABLE` query display.

Possible values:

- 0 — The query will be displayed without table UUID.
- 1 — The query will be displayed with table UUID.
)", 0) \
    DECLARE(Bool, database_atomic_wait_for_drop_and_detach_synchronously, false, R"(
Adds a modifier `SYNC` to all `DROP` and `DETACH` queries.

Possible values:

- 0 — Queries will be executed with delay.
- 1 — Queries will be executed without delay.
)", 0) \
    DECLARE(Bool, enable_scalar_subquery_optimization, true, R"(
If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.
)", 0) \
    DECLARE(Bool, optimize_trivial_count_query, true, R"(
Enables or disables the optimization to trivial query `SELECT count() FROM table` using metadata from MergeTree. If you need to use row-level security, disable this setting.

Possible values:

   - 0 — Optimization disabled.
   - 1 — Optimization enabled.

See also:

- [optimize_functions_to_subcolumns](#optimize-functions-to-subcolumns)
)", 0) \
    DECLARE(Bool, optimize_trivial_approximate_count_query, false, R"(
Use an approximate value for trivial count optimization of storages that support such estimation, for example, EmbeddedRocksDB.

Possible values:

   - 0 — Optimization disabled.
   - 1 — Optimization enabled.
)", 0) \
    DECLARE(Bool, optimize_count_from_files, true, R"(
Enables or disables the optimization of counting number of rows from files in different input formats. It applies to table functions/engines `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`.

Possible values:

- 0 — Optimization disabled.
- 1 — Optimization enabled.
)", 0) \
    DECLARE(Bool, use_cache_for_count_from_files, true, R"(
Enables caching of rows number during count from files in table functions `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`.

Enabled by default.
)", 0) \
    DECLARE(Bool, optimize_respect_aliases, true, R"(
If it is set to true, it will respect aliases in WHERE/GROUP BY/ORDER BY, that will help with partition pruning/secondary indexes/optimize_aggregation_in_order/optimize_read_in_order/optimize_trivial_count
)", 0) \
    DECLARE(UInt64, mutations_sync, 0, R"(
Allows to execute `ALTER TABLE ... UPDATE|DELETE|MATERIALIZE INDEX|MATERIALIZE PROJECTION|MATERIALIZE COLUMN` queries ([mutations](../../sql-reference/statements/alter/index.md#mutations)) synchronously.

Possible values:

- 0 - Mutations execute asynchronously.
- 1 - The query waits for all mutations to complete on the current server.
- 2 - The query waits for all mutations to complete on all replicas (if they exist).
)", 0) \
    DECLARE(Bool, enable_lightweight_delete, true, R"(
Enable lightweight DELETE mutations for mergetree tables.
)", 0) ALIAS(allow_experimental_lightweight_delete) \
    DECLARE(UInt64, lightweight_deletes_sync, 2, R"(
The same as [`mutations_sync`](#mutations_sync), but controls only execution of lightweight deletes.

Possible values:

- 0 - Mutations execute asynchronously.
- 1 - The query waits for the lightweight deletes to complete on the current server.
- 2 - The query waits for the lightweight deletes to complete on all replicas (if they exist).

**See Also**

- [Synchronicity of ALTER Queries](../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [Mutations](../../sql-reference/statements/alter/index.md#mutations)
)", 0) \
    DECLARE(Bool, apply_deleted_mask, true, R"(
Enables filtering out rows deleted with lightweight DELETE. If disabled, a query will be able to read those rows. This is useful for debugging and \"undelete\" scenarios
)", 0) \
    DECLARE(Bool, optimize_normalize_count_variants, true, R"(
Rewrite aggregate functions that semantically equals to count() as count().
)", 0) \
    DECLARE(Bool, optimize_injective_functions_inside_uniq, true, R"(
Delete injective functions of one argument inside uniq*() functions.
)", 0) \
    DECLARE(Bool, rewrite_count_distinct_if_with_count_distinct_implementation, false, R"(
Allows you to rewrite `countDistcintIf` with [count_distinct_implementation](#count_distinct_implementation) setting.

Possible values:

- true — Allow.
- false — Disallow.
)", 0) \
    DECLARE(Bool, convert_query_to_cnf, false, R"(
When set to `true`, a `SELECT` query will be converted to conjuctive normal form (CNF). There are scenarios where rewriting a query in CNF may execute faster (view this [Github issue](https://github.com/ClickHouse/ClickHouse/issues/11749) for an explanation).

For example, notice how the following `SELECT` query is not modified (the default behavior):

```sql
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT number AS x
    FROM numbers(20)
) AS a
WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))
SETTINGS convert_query_to_cnf = false;
```

The result is:

```response
┌─explain────────────────────────────────────────────────────────┐
│ SELECT x                                                       │
│ FROM                                                           │
│ (                                                              │
│     SELECT number AS x                                         │
│     FROM numbers(20)                                           │
│     WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) │
│ ) AS a                                                         │
│ WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))     │
│ SETTINGS convert_query_to_cnf = 0                              │
└────────────────────────────────────────────────────────────────┘
```

Let's set `convert_query_to_cnf` to `true` and see what changes:

```sql
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT number AS x
    FROM numbers(20)
) AS a
WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))
SETTINGS convert_query_to_cnf = true;
```

Notice the `WHERE` clause is rewritten in CNF, but the result set is the identical - the Boolean logic is unchanged:

```response
┌─explain───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ SELECT x                                                                                                              │
│ FROM                                                                                                                  │
│ (                                                                                                                     │
│     SELECT number AS x                                                                                                │
│     FROM numbers(20)                                                                                                  │
│     WHERE ((x <= 15) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x >= 10) OR (x >= 1)) │
│ ) AS a                                                                                                                │
│ WHERE ((x >= 10) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x <= 15) OR (x <= 5))     │
│ SETTINGS convert_query_to_cnf = 1                                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Possible values: true, false
)", 0) \
    DECLARE(Bool, optimize_or_like_chain, false, R"(
Optimize multiple OR LIKE into multiMatchAny. This optimization should not be enabled by default, because it defies index analysis in some cases.
)", 0) \
    DECLARE(Bool, optimize_arithmetic_operations_in_aggregate_functions, true, R"(
Move arithmetic operations out of aggregation functions
)", 0) \
    DECLARE(Bool, optimize_redundant_functions_in_order_by, true, R"(
Remove functions from ORDER BY if its argument is also in ORDER BY
)", 0) \
    DECLARE(Bool, optimize_if_chain_to_multiif, false, R"(
Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not beneficial for numeric types.
)", 0) \
    DECLARE(Bool, optimize_multiif_to_if, true, R"(
Replace 'multiIf' with only one condition to 'if'.
)", 0) \
    DECLARE(Bool, optimize_if_transform_strings_to_enum, false, R"(
Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in distributed query that would lead to its fail.
)", 0) \
    DECLARE(Bool, optimize_functions_to_subcolumns, true, R"(
Enables or disables optimization by transforming some functions to reading subcolumns. This reduces the amount of data to read.

These functions can be transformed:

- [length](../../sql-reference/functions/array-functions.md/#array_functions-length) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [empty](../../sql-reference/functions/array-functions.md/#function-empty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [notEmpty](../../sql-reference/functions/array-functions.md/#function-notempty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [isNull](../../sql-reference/operators/index.md#operator-is-null) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [isNotNull](../../sql-reference/operators/index.md#is-not-null) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [count](../../sql-reference/aggregate-functions/reference/count.md) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [mapKeys](../../sql-reference/functions/tuple-map-functions.md/#mapkeys) to read the [keys](../../sql-reference/data-types/map.md/#map-subcolumns) subcolumn.
- [mapValues](../../sql-reference/functions/tuple-map-functions.md/#mapvalues) to read the [values](../../sql-reference/data-types/map.md/#map-subcolumns) subcolumn.

Possible values:

- 0 — Optimization disabled.
- 1 — Optimization enabled.
)", 0) \
    DECLARE(Bool, optimize_using_constraints, false, R"(
Use [constraints](../../sql-reference/statements/create/table.md#constraints) for query optimization. The default is `false`.

Possible values:

- true, false
)", 0)                                                                                                                                           \
    DECLARE(Bool, optimize_substitute_columns, false, R"(
Use [constraints](../../sql-reference/statements/create/table.md#constraints) for column substitution. The default is `false`.

Possible values:

- true, false
)", 0)                                                                                                                                         \
    DECLARE(Bool, optimize_append_index, false, R"(
Use [constraints](../../sql-reference/statements/create/table.md#constraints) in order to append index condition. The default is `false`.

Possible values:

- true, false
)", 0) \
    DECLARE(Bool, optimize_time_filter_with_preimage, true, R"(
Optimize Date and DateTime predicates by converting functions into equivalent comparisons without conversions (e.g. toYear(col) = 2023 -> col >= '2023-01-01' AND col <= '2023-12-31')
)", 0) \
    DECLARE(Bool, normalize_function_names, true, R"(
Normalize function names to their canonical names
)", 0) \
    DECLARE(Bool, enable_early_constant_folding, true, R"(
Enable query optimization where we analyze function and subqueries results and rewrite query if there are constants there
)", 0) \
    DECLARE(Bool, deduplicate_blocks_in_dependent_materialized_views, false, R"(
Enables or disables the deduplication check for materialized views that receive data from Replicated\* tables.

Possible values:

      0 — Disabled.
      1 — Enabled.

Usage

By default, deduplication is not performed for materialized views but is done upstream, in the source table.
If an INSERTed block is skipped due to deduplication in the source table, there will be no insertion into attached materialized views. This behaviour exists to enable the insertion of highly aggregated data into materialized views, for cases where inserted blocks are the same after materialized view aggregation but derived from different INSERTs into the source table.
At the same time, this behaviour “breaks” `INSERT` idempotency. If an `INSERT` into the main table was successful and `INSERT` into a materialized view failed (e.g. because of communication failure with ClickHouse Keeper) a client will get an error and can retry the operation. However, the materialized view won’t receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` allows for changing this behaviour. On retry, a materialized view will receive the repeat insert and will perform a deduplication check by itself,
ignoring check result for the source table, and will insert rows lost because of the first failure.
)", 0) \
    DECLARE(Bool, throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert, true, R"(
Throw exception on INSERT query when the setting `deduplicate_blocks_in_dependent_materialized_views` is enabled along with `async_insert`. It guarantees correctness, because these features can't work together.
)", 0) \
    DECLARE(Bool, materialized_views_ignore_errors, false, R"(
Allows to ignore errors for MATERIALIZED VIEW, and deliver original block to the table regardless of MVs
)", 0) \
    DECLARE(Bool, ignore_materialized_views_with_dropped_target_table, false, R"(
Ignore MVs with dropped target table during pushing to views
)", 0) \
    DECLARE(Bool, allow_materialized_view_with_bad_select, true, R"(
Allow CREATE MATERIALIZED VIEW with SELECT query that references nonexistent tables or columns. It must still be syntactically valid. Doesn't apply to refreshable MVs. Doesn't apply if the MV schema needs to be inferred from the SELECT query (i.e. if the CREATE has no column list and no TO table). Can be used for creating MV before its source table.
)", 0) \
    DECLARE(Bool, use_compact_format_in_distributed_parts_names, true, R"(
Uses compact format for storing blocks for background (`distributed_foreground_insert`) INSERT into tables with `Distributed` engine.

Possible values:

- 0 — Uses `user[:password]@host:port#default_database` directory format.
- 1 — Uses `[shard{shard_index}[_replica{replica_index}]]` directory format.

:::note
- with `use_compact_format_in_distributed_parts_names=0` changes from cluster definition will not be applied for background INSERT.
- with `use_compact_format_in_distributed_parts_names=1` changing the order of the nodes in the cluster definition, will change the `shard_index`/`replica_index` so be aware.
:::
)", 0) \
    DECLARE(Bool, validate_polygons, true, R"(
Enables or disables throwing an exception in the [pointInPolygon](../../sql-reference/functions/geo/index.md#pointinpolygon) function, if the polygon is self-intersecting or self-tangent.

Possible values:

- 0 — Throwing an exception is disabled. `pointInPolygon` accepts invalid polygons and returns possibly incorrect results for them.
- 1 — Throwing an exception is enabled.
)", 0) \
    DECLARE(UInt64, max_parser_depth, DBMS_DEFAULT_MAX_PARSER_DEPTH, R"(
Limits maximum recursion depth in the recursive descent parser. Allows controlling the stack size.

Possible values:

- Positive integer.
- 0 — Recursion depth is unlimited.
)", 0) \
    DECLARE(UInt64, max_parser_backtracks, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS, R"(
Maximum parser backtracking (how many times it tries different alternatives in the recursive descend parsing process).
)", 0) \
    DECLARE(UInt64, max_recursive_cte_evaluation_depth, DBMS_RECURSIVE_CTE_MAX_EVALUATION_DEPTH, R"(
Maximum limit on recursive CTE evaluation depth
)", 0) \
    DECLARE(Bool, allow_settings_after_format_in_insert, false, R"(
Control whether `SETTINGS` after `FORMAT` in `INSERT` queries is allowed or not. It is not recommended to use this, since this may interpret part of `SETTINGS` as values.

Example:

```sql
INSERT INTO FUNCTION null('foo String') SETTINGS max_threads=1 VALUES ('bar');
```

But the following query will work only with `allow_settings_after_format_in_insert`:

```sql
SET allow_settings_after_format_in_insert=1;
INSERT INTO FUNCTION null('foo String') VALUES ('bar') SETTINGS max_threads=1;
```

Possible values:

- 0 — Disallow.
- 1 — Allow.

:::note
Use this setting only for backward compatibility if your use cases depend on old syntax.
:::
)", 0) \
    DECLARE(Seconds, periodic_live_view_refresh, 60, R"(
Interval after which periodically refreshed live view is forced to refresh.
)", 0) \
    DECLARE(Bool, transform_null_in, false, R"(
Enables equality of [NULL](../../sql-reference/syntax.md/#null-literal) values for [IN](../../sql-reference/operators/in.md) operator.

By default, `NULL` values can’t be compared because `NULL` means undefined value. Thus, comparison `expr = NULL` must always return `false`. With this setting `NULL = NULL` returns `true` for `IN` operator.

Possible values:

- 0 — Comparison of `NULL` values in `IN` operator returns `false`.
- 1 — Comparison of `NULL` values in `IN` operator returns `true`.

**Example**

Consider the `null_in` table:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
│    3 │     3 │
└──────┴───────┘
```

Query:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 0;
```

Result:

``` text
┌──idx─┬────i─┐
│    1 │    1 │
└──────┴──────┘
```

Query:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 1;
```

Result:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
└──────┴───────┘
```

**See Also**

- [NULL Processing in IN Operators](../../sql-reference/operators/in.md/#in-null-processing)
)", 0) \
    DECLARE(Bool, allow_nondeterministic_mutations, false, R"(
User-level setting that allows mutations on replicated tables to make use of non-deterministic functions such as `dictGet`.

Given that, for example, dictionaries, can be out of sync across nodes, mutations that pull values from them are disallowed on replicated tables by default. Enabling this setting allows this behavior, making it the user's responsibility to ensure that the data used is in sync across all nodes.

**Example**

``` xml
<profiles>
    <default>
        <allow_nondeterministic_mutations>1</allow_nondeterministic_mutations>

        <!-- ... -->
    </default>

    <!-- ... -->

</profiles>
```
)", 0) \
    DECLARE(Seconds, lock_acquire_timeout, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, R"(
Defines how many seconds a locking request waits before failing.

Locking timeout is used to protect from deadlocks while executing read/write operations with tables. When the timeout expires and the locking request fails, the ClickHouse server throws an exception "Locking attempt timed out! Possible deadlock avoided. Client should retry." with error code `DEADLOCK_AVOIDED`.

Possible values:

- Positive integer (in seconds).
- 0 — No locking timeout.
)", 0) \
    DECLARE(Bool, materialize_ttl_after_modify, true, R"(
Apply TTL for old data, after ALTER MODIFY TTL query
)", 0) \
    DECLARE(String, function_implementation, "", R"(
Choose function implementation for specific target or variant (experimental). If empty enable all of them.
)", 0) \
    DECLARE(Bool, data_type_default_nullable, false, R"(
Allows data types without explicit modifiers [NULL or NOT NULL](../../sql-reference/statements/create/table.md/#null-modifiers) in column definition will be [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable).

Possible values:

- 1 — The data types in column definitions are set to `Nullable` by default.
- 0 — The data types in column definitions are set to not `Nullable` by default.
)", 0) \
    DECLARE(Bool, cast_keep_nullable, false, R"(
Enables or disables keeping of the `Nullable` data type in [CAST](../../sql-reference/functions/type-conversion-functions.md/#castx-t) operations.

When the setting is enabled and the argument of `CAST` function is `Nullable`, the result is also transformed to `Nullable` type. When the setting is disabled, the result always has the destination type exactly.

Possible values:

- 0 — The `CAST` result has exactly the destination type specified.
- 1 — If the argument type is `Nullable`, the `CAST` result is transformed to `Nullable(DestinationDataType)`.

**Examples**

The following query results in the destination data type exactly:

```sql
SET cast_keep_nullable = 0;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Result:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Int32                                             │
└───┴───────────────────────────────────────────────────┘
```

The following query results in the `Nullable` modification on the destination data type:

```sql
SET cast_keep_nullable = 1;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Result:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Nullable(Int32)                                   │
└───┴───────────────────────────────────────────────────┘
```

**See Also**

- [CAST](../../sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) function
)", 0) \
    DECLARE(Bool, cast_ipv4_ipv6_default_on_conversion_error, false, R"(
CAST operator into IPv4, CAST operator into IPV6 type, toIPv4, toIPv6 functions will return default value instead of throwing exception on conversion error.
)", 0) \
    DECLARE(Bool, alter_partition_verbose_result, false, R"(
Enables or disables the display of information about the parts to which the manipulation operations with partitions and parts have been successfully applied.
Applicable to [ATTACH PARTITION|PART](../../sql-reference/statements/alter/partition.md/#alter_attach-partition) and to [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md/#alter_freeze-partition).

Possible values:

- 0 — disable verbosity.
- 1 — enable verbosity.

**Example**

```sql
CREATE TABLE test(a Int64, d Date, s String) ENGINE = MergeTree PARTITION BY toYYYYMDECLARE(d) ORDER BY a;
INSERT INTO test VALUES(1, '2021-01-01', '');
INSERT INTO test VALUES(1, '2021-01-01', '');
ALTER TABLE test DETACH PARTITION ID '202101';

ALTER TABLE test ATTACH PARTITION ID '202101' SETTINGS alter_partition_verbose_result = 1;

┌─command_type─────┬─partition_id─┬─part_name────┬─old_part_name─┐
│ ATTACH PARTITION │ 202101       │ 202101_7_7_0 │ 202101_5_5_0  │
│ ATTACH PARTITION │ 202101       │ 202101_8_8_0 │ 202101_6_6_0  │
└──────────────────┴──────────────┴──────────────┴───────────────┘

ALTER TABLE test FREEZE SETTINGS alter_partition_verbose_result = 1;

┌─command_type─┬─partition_id─┬─part_name────┬─backup_name─┬─backup_path───────────────────┬─part_backup_path────────────────────────────────────────────┐
│ FREEZE ALL   │ 202101       │ 202101_7_7_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_7_7_0 │
│ FREEZE ALL   │ 202101       │ 202101_8_8_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_8_8_0 │
└──────────────┴──────────────┴──────────────┴─────────────┴───────────────────────────────┴─────────────────────────────────────────────────────────────┘
```
)", 0) \
    DECLARE(Bool, system_events_show_zero_values, false, R"(
Allows to select zero-valued events from [`system.events`](../../operations/system-tables/events.md).

Some monitoring systems require passing all the metrics values to them for each checkpoint, even if the metric value is zero.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Examples**

Query

```sql
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Result

```text
Ok.
```

Query
```sql
SET system_events_show_zero_values = 1;
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Result

```text
┌─event────────────────────┬─value─┬─description───────────────────────────────────────────┐
│ QueryMemoryLimitExceeded │     0 │ Number of times when memory limit exceeded for query. │
└──────────────────────────┴───────┴───────────────────────────────────────────────────────┘
```
)", 0) \
    DECLARE(MySQLDataTypesSupport, mysql_datatypes_support_level, MySQLDataTypesSupportList{}, R"(
Defines how MySQL types are converted to corresponding ClickHouse types. A comma separated list in any combination of `decimal`, `datetime64`, `date2Date32` or `date2String`.
- `decimal`: convert `NUMERIC` and `DECIMAL` types to `Decimal` when precision allows it.
- `datetime64`: convert `DATETIME` and `TIMESTAMP` types to `DateTime64` instead of `DateTime` when precision is not `0`.
- `date2Date32`: convert `DATE` to `Date32` instead of `Date`. Takes precedence over `date2String`.
- `date2String`: convert `DATE` to `String` instead of `Date`. Overridden by `datetime64`.
)", 0) \
    DECLARE(Bool, optimize_trivial_insert_select, false, R"(
Optimize trivial 'INSERT INTO table SELECT ... FROM TABLES' query
)", 0) \
    DECLARE(Bool, allow_non_metadata_alters, true, R"(
Allow to execute alters which affects not only tables metadata, but also data on disk
)", 0) \
    DECLARE(Bool, enable_global_with_statement, true, R"(
Propagate WITH statements to UNION queries and all subqueries
)", 0) \
    DECLARE(Bool, aggregate_functions_null_for_empty, false, R"(
Enables or disables rewriting all aggregate functions in a query, adding [-OrNull](../../sql-reference/aggregate-functions/combinators.md/#agg-functions-combinator-ornull) suffix to them. Enable it for SQL standard compatibility.
It is implemented via query rewrite (similar to [count_distinct_implementation](#count_distinct_implementation) setting) to get consistent results for distributed queries.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

Consider the following query with aggregate functions:
```sql
SELECT SUM(-1), MAX(0) FROM system.one WHERE 0;
```

With `aggregate_functions_null_for_empty = 0` it would produce:
```text
┌─SUM(-1)─┬─MAX(0)─┐
│       0 │      0 │
└─────────┴────────┘
```

With `aggregate_functions_null_for_empty = 1` the result would be:
```text
┌─SUMOrNull(-1)─┬─MAXOrNull(0)─┐
│          NULL │         NULL │
└───────────────┴──────────────┘
```
)", 0) \
    DECLARE(Bool, optimize_syntax_fuse_functions, false, R"(
Enables to fuse aggregate functions with identical argument. It rewrites query contains at least two aggregate functions from [sum](../../sql-reference/aggregate-functions/reference/sum.md/#agg_function-sum), [count](../../sql-reference/aggregate-functions/reference/count.md/#agg_function-count) or [avg](../../sql-reference/aggregate-functions/reference/avg.md/#agg_function-avg) with identical argument to [sumCount](../../sql-reference/aggregate-functions/reference/sumcount.md/#agg_function-sumCount).

Possible values:

- 0 — Functions with identical argument are not fused.
- 1 — Functions with identical argument are fused.

**Example**

Query:

``` sql
CREATE TABLE fuse_tbl(a Int8, b Int8) Engine = Log;
SET optimize_syntax_fuse_functions = 1;
EXPLAIN SYNTAX SELECT sum(a), sum(b), count(b), avg(b) from fuse_tbl FORMAT TSV;
```

Result:

``` text
SELECT
    sum(a),
    sumCount(b).1,
    sumCount(b).2,
    (sumCount(b).1) / (sumCount(b).2)
FROM fuse_tbl
```
)", 0) \
    DECLARE(Bool, flatten_nested, true, R"(
Sets the data format of a [nested](../../sql-reference/data-types/nested-data-structures/index.md) columns.

Possible values:

- 1 — Nested column is flattened to separate arrays.
- 0 — Nested column stays a single array of tuples.

**Usage**

If the setting is set to `0`, it is possible to use an arbitrary level of nesting.

**Examples**

Query:

``` sql
SET flatten_nested = 1;
CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Result:

``` text
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n.a` Array(UInt32),
    `n.b` Array(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SET flatten_nested = 0;

CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Result:

``` text
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n` Nested(a UInt32, b UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
)", 0) \
    DECLARE(Bool, asterisk_include_materialized_columns, false, R"(
Include [MATERIALIZED](../../sql-reference/statements/create/table.md#materialized) columns for wildcard query (`SELECT *`).

Possible values:

- 0 - disabled
- 1 - enabled
)", 0) \
    DECLARE(Bool, asterisk_include_alias_columns, false, R"(
Include [ALIAS](../../sql-reference/statements/create/table.md#alias) columns for wildcard query (`SELECT *`).

Possible values:

- 0 - disabled
- 1 - enabled
)", 0) \
    DECLARE(Bool, optimize_skip_merged_partitions, false, R"(
Enables or disables optimization for [OPTIMIZE TABLE ... FINAL](../../sql-reference/statements/optimize.md) query if there is only one part with level > 0 and it doesn't have expired TTL.

- `OPTIMIZE TABLE ... FINAL SETTINGS optimize_skip_merged_partitions=1`

By default, `OPTIMIZE TABLE ... FINAL` query rewrites the one part even if there is only a single part.

Possible values:

- 1 - Enable optimization.
- 0 - Disable optimization.
)", 0) \
    DECLARE(Bool, optimize_on_insert, true, R"(
Enables or disables data transformation before the insertion, as if merge was done on this block (according to table engine).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

The difference between enabled and disabled:

Query:

```sql
SET optimize_on_insert = 1;

CREATE TABLE test1 (`FirstTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY FirstTable;

INSERT INTO test1 SELECT number % 2 FROM numbers(5);

SELECT * FROM test1;

SET optimize_on_insert = 0;

CREATE TABLE test2 (`SecondTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY SecondTable;

INSERT INTO test2 SELECT number % 2 FROM numbers(5);

SELECT * FROM test2;
```

Result:

``` text
┌─FirstTable─┐
│          0 │
│          1 │
└────────────┘

┌─SecondTable─┐
│           0 │
│           0 │
│           0 │
│           1 │
│           1 │
└─────────────┘
```

Note that this setting influences [Materialized view](../../sql-reference/statements/create/view.md/#materialized) and [MaterializedMySQL](../../engines/database-engines/materialized-mysql.md) behaviour.
)", 0) \
    DECLARE(Bool, optimize_use_projections, true, R"(
Enables or disables [projection](../../engines/table-engines/mergetree-family/mergetree.md/#projections) optimization when processing `SELECT` queries.

Possible values:

- 0 — Projection optimization disabled.
- 1 — Projection optimization enabled.
)", 0) ALIAS(allow_experimental_projection_optimization) \
    DECLARE(Bool, optimize_use_implicit_projections, true, R"(
Automatically choose implicit projections to perform SELECT query
)", 0) \
    DECLARE(Bool, force_optimize_projection, false, R"(
Enables or disables the obligatory use of [projections](../../engines/table-engines/mergetree-family/mergetree.md/#projections) in `SELECT` queries, when projection optimization is enabled (see [optimize_use_projections](#optimize_use_projections) setting).

Possible values:

- 0 — Projection optimization is not obligatory.
- 1 — Projection optimization is obligatory.
)", 0) \
    DECLARE(String, force_optimize_projection_name, "", R"(
If it is set to a non-empty string, check that this projection is used in the query at least once.

Possible values:

- string: name of projection that used in a query
)", 0) \
    DECLARE(String, preferred_optimize_projection_name, "", R"(
If it is set to a non-empty string, ClickHouse will try to apply specified projection in query.


Possible values:

- string: name of preferred projection
)", 0) \
    DECLARE(Bool, async_socket_for_remote, true, R"(
Enables asynchronous read from socket while executing remote query.

Enabled by default.
)", 0) \
    DECLARE(Bool, async_query_sending_for_remote, true, R"(
Enables asynchronous connection creation and query sending while executing remote query.

Enabled by default.
)", 0) \
    DECLARE(Bool, insert_null_as_default, true, R"(
Enables or disables the insertion of [default values](../../sql-reference/statements/create/table.md/#create-default-values) instead of [NULL](../../sql-reference/syntax.md/#null-literal) into columns with not [nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable) data type.
If column type is not nullable and this setting is disabled, then inserting `NULL` causes an exception. If column type is nullable, then `NULL` values are inserted as is, regardless of this setting.

This setting is applicable to [INSERT ... SELECT](../../sql-reference/statements/insert-into.md/#inserting-the-results-of-select) queries. Note that `SELECT` subqueries may be concatenated with `UNION ALL` clause.

Possible values:

- 0 — Inserting `NULL` into a not nullable column causes an exception.
- 1 — Default column value is inserted instead of `NULL`.
)", 0) \
    DECLARE(Bool, describe_extend_object_types, false, R"(
Deduce concrete type of columns of type Object in DESCRIBE query
)", 0) \
    DECLARE(Bool, describe_include_subcolumns, false, R"(
Enables describing subcolumns for a [DESCRIBE](../../sql-reference/statements/describe-table.md) query. For example, members of a [Tuple](../../sql-reference/data-types/tuple.md) or subcolumns of a [Map](../../sql-reference/data-types/map.md/#map-subcolumns), [Nullable](../../sql-reference/data-types/nullable.md/#finding-null) or an [Array](../../sql-reference/data-types/array.md/#array-size) data type.

Possible values:

- 0 — Subcolumns are not included in `DESCRIBE` queries.
- 1 — Subcolumns are included in `DESCRIBE` queries.

**Example**

See an example for the [DESCRIBE](../../sql-reference/statements/describe-table.md) statement.
)", 0) \
    DECLARE(Bool, describe_include_virtual_columns, false, R"(
If true, virtual columns of table will be included into result of DESCRIBE query
)", 0) \
    DECLARE(Bool, describe_compact_output, false, R"(
If true, include only column names and types into result of DESCRIBE query
)", 0) \
    DECLARE(Bool, apply_mutations_on_fly, false, R"(
If true, mutations (UPDATEs and DELETEs) which are not materialized in data part will be applied on SELECTs. Only available in ClickHouse Cloud.
)", 0) \
    DECLARE(Bool, mutations_execute_nondeterministic_on_initiator, false, R"(
If true constant nondeterministic functions (e.g. function `now()`) are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. It helps to keep data in sync on replicas while executing mutations with constant nondeterministic functions. Default value: `false`.
)", 0) \
    DECLARE(Bool, mutations_execute_subqueries_on_initiator, false, R"(
If true scalar subqueries are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. Default value: `false`.
)", 0) \
    DECLARE(UInt64, mutations_max_literal_size_to_replace, 16384, R"(
The maximum size of serialized literal in bytes to replace in `UPDATE` and `DELETE` queries. Takes effect only if at least one the two settings above is enabled. Default value: 16384 (16 KiB).
)", 0) \
    \
    DECLARE(Float, create_replicated_merge_tree_fault_injection_probability, 0.0f, R"(
The probability of a fault injection during table creation after creating metadata in ZooKeeper
)", 0) \
    \
    DECLARE(Bool, use_query_cache, false, R"(
If turned on, `SELECT` queries may utilize the [query cache](../query-cache.md). Parameters [enable_reads_from_query_cache](#enable-reads-from-query-cache)
and [enable_writes_to_query_cache](#enable-writes-to-query-cache) control in more detail how the cache is used.

Possible values:

- 0 - Disabled
- 1 - Enabled
)", 0) \
    DECLARE(Bool, enable_writes_to_query_cache, true, R"(
If turned on, results of `SELECT` queries are stored in the [query cache](../query-cache.md).

Possible values:

- 0 - Disabled
- 1 - Enabled
)", 0) \
    DECLARE(Bool, enable_reads_from_query_cache, true, R"(
If turned on, results of `SELECT` queries are retrieved from the [query cache](../query-cache.md).

Possible values:

- 0 - Disabled
- 1 - Enabled
)", 0) \
    DECLARE(QueryCacheNondeterministicFunctionHandling, query_cache_nondeterministic_function_handling, QueryCacheNondeterministicFunctionHandling::Throw, R"(
Controls how the [query cache](../query-cache.md) handles `SELECT` queries with non-deterministic functions like `rand()` or `now()`.

Possible values:

- `'throw'` - Throw an exception and don't cache the query result.
- `'save'` - Cache the query result.
- `'ignore'` - Don't cache the query result and don't throw an exception.
)", 0) \
    DECLARE(QueryCacheSystemTableHandling, query_cache_system_table_handling, QueryCacheSystemTableHandling::Throw, R"(
Controls how the [query cache](../query-cache.md) handles `SELECT` queries against system tables, i.e. tables in databases `system.*` and `information_schema.*`.

Possible values:

- `'throw'` - Throw an exception and don't cache the query result.
- `'save'` - Cache the query result.
- `'ignore'` - Don't cache the query result and don't throw an exception.
)", 0) \
    DECLARE(UInt64, query_cache_max_size_in_bytes, 0, R"(
The maximum amount of memory (in bytes) the current user may allocate in the [query cache](../query-cache.md). 0 means unlimited.

Possible values:

- Positive integer >= 0.
)", 0) \
    DECLARE(UInt64, query_cache_max_entries, 0, R"(
The maximum number of query results the current user may store in the [query cache](../query-cache.md). 0 means unlimited.

Possible values:

- Positive integer >= 0.
)", 0) \
    DECLARE(UInt64, query_cache_min_query_runs, 0, R"(
Minimum number of times a `SELECT` query must run before its result is stored in the [query cache](../query-cache.md).

Possible values:

- Positive integer >= 0.
)", 0) \
    DECLARE(Milliseconds, query_cache_min_query_duration, 0, R"(
Minimum duration in milliseconds a query needs to run for its result to be stored in the [query cache](../query-cache.md).

Possible values:

- Positive integer >= 0.
)", 0) \
    DECLARE(Bool, query_cache_compress_entries, true, R"(
Compress entries in the [query cache](../query-cache.md). Lessens the memory consumption of the query cache at the cost of slower inserts into / reads from it.

Possible values:

- 0 - Disabled
- 1 - Enabled
)", 0) \
    DECLARE(Bool, query_cache_squash_partial_results, true, R"(
Squash partial result blocks to blocks of size [max_block_size](#setting-max_block_size). Reduces performance of inserts into the [query cache](../query-cache.md) but improves the compressability of cache entries (see [query_cache_compress-entries](#query-cache-compress-entries)).

Possible values:

- 0 - Disabled
- 1 - Enabled
)", 0) \
    DECLARE(Seconds, query_cache_ttl, 60, R"(
After this time in seconds entries in the [query cache](../query-cache.md) become stale.

Possible values:

- Positive integer >= 0.
)", 0) \
    DECLARE(Bool, query_cache_share_between_users, false, R"(
If turned on, the result of `SELECT` queries cached in the [query cache](../query-cache.md) can be read by other users.
It is not recommended to enable this setting due to security reasons.

Possible values:

- 0 - Disabled
- 1 - Enabled
)", 0) \
    DECLARE(String, query_cache_tag, "", R"(
A string which acts as a label for [query cache](../query-cache.md) entries.
The same queries with different tags are considered different by the query cache.

Possible values:

- Any string
)", 0) \
    DECLARE(Bool, enable_sharing_sets_for_mutations, true, R"(
Allow sharing set objects build for IN subqueries between different tasks of the same mutation. This reduces memory usage and CPU consumption
)", 0) \
    \
    DECLARE(Bool, optimize_rewrite_sum_if_to_count_if, true, R"(
Rewrite sumIf() and sum(if()) function countIf() function when logically equivalent
)", 0) \
    DECLARE(Bool, optimize_rewrite_aggregate_function_with_if, true, R"(
Rewrite aggregate functions with if expression as argument when logically equivalent.
For example, `avg(if(cond, col, null))` can be rewritten to `avgOrNullIf(cond, col)`. It may improve performance.

:::note
Supported only with experimental analyzer (`enable_analyzer = 1`).
:::
)", 0) \
    DECLARE(Bool, optimize_rewrite_array_exists_to_has, false, R"(
Rewrite arrayExists() functions to has() when logically equivalent. For example, arrayExists(x -> x = 1, arr) can be rewritten to has(arr, 1)
)", 0) \
    DECLARE(UInt64, insert_shard_id, 0, R"(
If not `0`, specifies the shard of [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table into which the data will be inserted synchronously.

If `insert_shard_id` value is incorrect, the server will throw an exception.

To get the number of shards on `requested_cluster`, you can check server config or use this query:

``` sql
SELECT uniq(shard_num) FROM system.clusters WHERE cluster = 'requested_cluster';
```

Possible values:

- 0 — Disabled.
- Any number from `1` to `shards_num` of corresponding [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table.

**Example**

Query:

```sql
CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;
CREATE TABLE x_dist AS x ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), x);
INSERT INTO x_dist SELECT * FROM numbers(5) SETTINGS insert_shard_id = 1;
SELECT * FROM x_dist ORDER BY number ASC;
```

Result:

``` text
┌─number─┐
│      0 │
│      0 │
│      1 │
│      1 │
│      2 │
│      2 │
│      3 │
│      3 │
│      4 │
│      4 │
└────────┘
```
)", 0) \
    \
    DECLARE(Bool, collect_hash_table_stats_during_aggregation, true, R"(
Enable collecting hash table statistics to optimize memory allocation
)", 0) \
    DECLARE(UInt64, max_size_to_preallocate_for_aggregation, 100'000'000, R"(
For how many elements it is allowed to preallocate space in all hash tables in total before aggregation
)", 0) \
    \
    DECLARE(Bool, collect_hash_table_stats_during_joins, true, R"(
Enable collecting hash table statistics to optimize memory allocation
)", 0) \
    DECLARE(UInt64, max_size_to_preallocate_for_joins, 100'000'000, R"(
For how many elements it is allowed to preallocate space in all hash tables in total before join
)", 0) \
    \
    DECLARE(Bool, kafka_disable_num_consumers_limit, false, R"(
Disable limit on kafka_num_consumers that depends on the number of available CPU cores.
)", 0) \
    DECLARE(Bool, allow_experimental_kafka_offsets_storage_in_keeper, false, R"(
Allow experimental feature to store Kafka related offsets in ClickHouse Keeper. When enabled a ClickHouse Keeper path and replica name can be specified to the Kafka table engine. As a result instead of the regular Kafka engine, a new type of storage engine will be used that stores the committed offsets primarily in ClickHouse Keeper
)", 0) \
    DECLARE(Bool, enable_software_prefetch_in_aggregation, true, R"(
Enable use of software prefetch in aggregation
)", 0) \
    DECLARE(Bool, allow_aggregate_partitions_independently, false, R"(
Enable independent aggregation of partitions on separate threads when partition key suits group by key. Beneficial when number of partitions close to number of cores and partitions have roughly the same size
)", 0) \
    DECLARE(Bool, force_aggregate_partitions_independently, false, R"(
Force the use of optimization when it is applicable, but heuristics decided not to use it
)", 0) \
    DECLARE(UInt64, max_number_of_partitions_for_independent_aggregation, 128, R"(
Maximal number of partitions in table to apply optimization
)", 0) \
    DECLARE(Float, min_hit_rate_to_use_consecutive_keys_optimization, 0.5, R"(
Minimal hit rate of a cache which is used for consecutive keys optimization in aggregation to keep it enabled
)", 0) \
    \
    DECLARE(Bool, engine_file_empty_if_not_exists, false, R"(
Allows to select data from a file engine table without file.

Possible values:
- 0 — `SELECT` throws exception.
- 1 — `SELECT` returns empty result.
)", 0) \
    DECLARE(Bool, engine_file_truncate_on_insert, false, R"(
Enables or disables truncate before insert in [File](../../engines/table-engines/special/file.md) engine tables.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.
)", 0) \
    DECLARE(Bool, engine_file_allow_create_multiple_files, false, R"(
Enables or disables creating a new file on each insert in file engine tables if the format has the suffix (`JSON`, `ORC`, `Parquet`, etc.). If enabled, on each insert a new file will be created with a name following this pattern:

`data.Parquet` -> `data.1.Parquet` -> `data.2.Parquet`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.
)", 0) \
    DECLARE(Bool, engine_file_skip_empty_files, false, R"(
Enables or disables skipping empty files in [File](../../engines/table-engines/special/file.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.
)", 0) \
    DECLARE(Bool, engine_url_skip_empty_files, false, R"(
Enables or disables skipping empty files in [URL](../../engines/table-engines/special/url.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.
)", 0) \
    DECLARE(Bool, enable_url_encoding, true, R"(
Allows to enable/disable decoding/encoding path in uri in [URL](../../engines/table-engines/special/url.md) engine tables.

Enabled by default.
)", 0) \
    DECLARE(UInt64, database_replicated_initial_query_timeout_sec, 300, R"(
Sets how long initial DDL query should wait for Replicated database to process previous DDL queue entries in seconds.

Possible values:

- Positive integer.
- 0 — Unlimited.
)", 0) \
    DECLARE(Bool, database_replicated_enforce_synchronous_settings, false, R"(
Enforces synchronous waiting for some queries (see also database_atomic_wait_for_drop_and_detach_synchronously, mutation_sync, alter_sync). Not recommended to enable these settings.
)", 0) \
    DECLARE(UInt64, max_distributed_depth, 5, R"(
Limits the maximum depth of recursive queries for [Distributed](../../engines/table-engines/special/distributed.md) tables.

If the value is exceeded, the server throws an exception.

Possible values:

- Positive integer.
- 0 — Unlimited depth.
)", 0) \
    DECLARE(Bool, database_replicated_always_detach_permanently, false, R"(
Execute DETACH TABLE as DETACH TABLE PERMANENTLY if database engine is Replicated
)", 0) \
    DECLARE(Bool, database_replicated_allow_only_replicated_engine, false, R"(
Allow to create only Replicated tables in database with engine Replicated
)", 0) \
    DECLARE(UInt64, database_replicated_allow_replicated_engine_arguments, 0, R"(
0 - Don't allow to explicitly specify ZooKeeper path and replica name for *MergeTree tables in Replicated databases. 1 - Allow. 2 - Allow, but ignore the specified path and use default one instead. 3 - Allow and don't log a warning.
)", 0) \
    DECLARE(UInt64, database_replicated_allow_explicit_uuid, 0, R"(
0 - Don't allow to explicitly specify UUIDs for tables in Replicated databases. 1 - Allow. 2 - Allow, but ignore the specified UUID and generate a random one instead.
)", 0) \
    DECLARE(Bool, database_replicated_allow_heavy_create, false, R"(
Allow long-running DDL queries (CREATE AS SELECT and POPULATE) in Replicated database engine. Note that it can block DDL queue for a long time.
)", 0) \
    DECLARE(Bool, cloud_mode, false, R"(
Cloud mode
)", 0) \
    DECLARE(UInt64, cloud_mode_engine, 1, R"(
The engine family allowed in Cloud. 0 - allow everything, 1 - rewrite DDLs to use *ReplicatedMergeTree, 2 - rewrite DDLs to use SharedMergeTree. UInt64 to minimize public part
)", 0) \
    DECLARE(UInt64, cloud_mode_database_engine, 1, R"(
The database engine allowed in Cloud. 1 - rewrite DDLs to use Replicated database, 2 - rewrite DDLs to use Shared database
)", 0) \
    DECLARE(DistributedDDLOutputMode, distributed_ddl_output_mode, DistributedDDLOutputMode::THROW, R"(
Sets format of distributed DDL query result.

Possible values:

- `throw` — Returns result set with query execution status for all hosts where query is finished. If query has failed on some hosts, then it will rethrow the first exception. If query is not finished yet on some hosts and [distributed_ddl_task_timeout](#distributed_ddl_task_timeout) exceeded, then it throws `TIMEOUT_EXCEEDED` exception.
- `none` — Is similar to throw, but distributed DDL query returns no result set.
- `null_status_on_timeout` — Returns `NULL` as execution status in some rows of result set instead of throwing `TIMEOUT_EXCEEDED` if query is not finished on the corresponding hosts.
- `never_throw` — Do not throw `TIMEOUT_EXCEEDED` and do not rethrow exceptions if query has failed on some hosts.
- `none_only_active` - similar to `none`, but doesn't wait for inactive replicas of the `Replicated` database. Note: with this mode it's impossible to figure out that the query was not executed on some replica and will be executed in background.
- `null_status_on_timeout_only_active` — similar to `null_status_on_timeout`, but doesn't wait for inactive replicas of the `Replicated` database
- `throw_only_active` — similar to `throw`, but doesn't wait for inactive replicas of the `Replicated` database

Cloud default value: `none`.
)", 0) \
    DECLARE(UInt64, distributed_ddl_entry_format_version, 5, R"(
Compatibility version of distributed DDL (ON CLUSTER) queries
)", 0) \
    \
    DECLARE(UInt64, external_storage_max_read_rows, 0, R"(
Limit maximum number of rows when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled
)", 0) \
    DECLARE(UInt64, external_storage_max_read_bytes, 0, R"(
Limit maximum number of bytes when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled
)", 0)  \
    DECLARE(UInt64, external_storage_connect_timeout_sec, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, R"(
Connect timeout in seconds. Now supported only for MySQL
)", 0)  \
    DECLARE(UInt64, external_storage_rw_timeout_sec, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, R"(
Read/write timeout in seconds. Now supported only for MySQL
)", 0)  \
    \
    DECLARE(SetOperationMode, union_default_mode, SetOperationMode::Unspecified, R"(
Sets a mode for combining `SELECT` query results. The setting is only used when shared with [UNION](../../sql-reference/statements/select/union.md) without explicitly specifying the `UNION ALL` or `UNION DISTINCT`.

Possible values:

- `'DISTINCT'` — ClickHouse outputs rows as a result of combining queries removing duplicate rows.
- `'ALL'` — ClickHouse outputs all rows as a result of combining queries including duplicate rows.
- `''` — ClickHouse generates an exception when used with `UNION`.

See examples in [UNION](../../sql-reference/statements/select/union.md).
)", 0) \
    DECLARE(SetOperationMode, intersect_default_mode, SetOperationMode::ALL, R"(
Set default mode in INTERSECT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.
)", 0) \
    DECLARE(SetOperationMode, except_default_mode, SetOperationMode::ALL, R"(
Set default mode in EXCEPT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.
)", 0) \
    DECLARE(Bool, optimize_aggregators_of_group_by_keys, true, R"(
Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section
)", 0) \
    DECLARE(Bool, optimize_injective_functions_in_group_by, true, R"(
Replaces injective functions by it's arguments in GROUP BY section
)", 0) \
    DECLARE(Bool, optimize_group_by_function_keys, true, R"(
Eliminates functions of other keys in GROUP BY section
)", 0) \
    DECLARE(Bool, optimize_group_by_constant_keys, true, R"(
Optimize GROUP BY when all keys in block are constant
)", 0) \
    DECLARE(Bool, legacy_column_name_of_tuple_literal, false, R"(
List all names of element of large tuple literals in their column names instead of hash. This settings exists only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher.
)", 0) \
    DECLARE(Bool, enable_named_columns_in_function_tuple, false, R"(
Generate named tuples in function tuple() when all names are unique and can be treated as unquoted identifiers.
)", 0) \
    \
    DECLARE(Bool, query_plan_enable_optimizations, true, R"(
Toggles query optimization at the query plan level.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable all optimizations at the query plan level
- 1 - Enable optimizations at the query plan level (but individual optimizations may still be disabled via their individual settings)
)", 0) \
    DECLARE(UInt64, query_plan_max_optimizations_to_apply, 10000, R"(
Limits the total number of optimizations applied to query plan, see setting [query_plan_enable_optimizations](#query_plan_enable_optimizations).
Useful to avoid long optimization times for complex queries.
If the actual number of optimizations exceeds this setting, an exception is thrown.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::
)", 0) \
    DECLARE(Bool, query_plan_lift_up_array_join, true, R"(
Toggles a query-plan-level optimization which moves ARRAY JOINs up in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_push_down_limit, true, R"(
Toggles a query-plan-level optimization which moves LIMITs down in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_split_filter, true, R"(
:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Toggles a query-plan-level optimization which splits filters into expressions.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_merge_expressions, true, R"(
Toggles a query-plan-level optimization which merges consecutive filters.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_merge_filters, false, R"(
Allow to merge filters in the query plan
)", 0) \
    DECLARE(Bool, query_plan_filter_push_down, true, R"(
Toggles a query-plan-level optimization which moves filters down in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_convert_outer_join_to_inner_join, true, R"(
Allow to convert OUTER JOIN to INNER JOIN if filter after JOIN always filters default values
)", 0) \
    DECLARE(Bool, query_plan_optimize_prewhere, true, R"(
Allow to push down filter to PREWHERE expression for supported storages
)", 0) \
    DECLARE(Bool, query_plan_execute_functions_after_sorting, true, R"(
Toggles a query-plan-level optimization which moves expressions after sorting steps.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_reuse_storage_ordering_for_window_functions, true, R"(
Toggles a query-plan-level optimization which uses storage sorting when sorting for window functions.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_lift_up_union, true, R"(
Toggles a query-plan-level optimization which moves larger subtrees of the query plan into union to enable further optimizations.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_read_in_order, true, R"(
Toggles the read in-order optimization query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_aggregation_in_order, true, R"(
Toggles the aggregation in-order query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_remove_redundant_sorting, true, R"(
Toggles a query-plan-level optimization which removes redundant sorting steps, e.g. in subqueries.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_remove_redundant_distinct, true, R"(
Toggles a query-plan-level optimization which removes redundant DISTINCT steps.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable
)", 0) \
    DECLARE(Bool, query_plan_enable_multithreading_after_window_functions, true, R"(
Enable multithreading after evaluating window functions to allow parallel stream processing
)", 0) \
    DECLARE(UInt64, regexp_max_matches_per_row, 1000, R"(
Sets the maximum number of matches for a single regular expression per row. Use it to protect against memory overload when using greedy regular expression in the [extractAllGroupsHorizontal](../../sql-reference/functions/string-search-functions.md/#extractallgroups-horizontal) function.

Possible values:

- Positive integer.
)", 0) \
    \
    DECLARE(UInt64, limit, 0, R"(
Sets the maximum number of rows to get from the query result. It adjusts the value set by the [LIMIT](../../sql-reference/statements/select/limit.md/#limit-clause) clause, so that the limit, specified in the query, cannot exceed the limit, set by this setting.

Possible values:

- 0 — The number of rows is not limited.
- Positive integer.
)", 0) \
    DECLARE(UInt64, offset, 0, R"(
Sets the number of rows to skip before starting to return rows from the query. It adjusts the offset set by the [OFFSET](../../sql-reference/statements/select/offset.md/#offset-fetch) clause, so that these two values are summarized.

Possible values:

- 0 — No rows are skipped .
- Positive integer.

**Example**

Input table:

``` sql
CREATE TABLE test (i UInt64) ENGINE = MergeTree() ORDER BY i;
INSERT INTO test SELECT number FROM numbers(500);
```

Query:

``` sql
SET limit = 5;
SET offset = 7;
SELECT * FROM test LIMIT 10 OFFSET 100;
```
Result:

``` text
┌───i─┐
│ 107 │
│ 108 │
│ 109 │
└─────┘
```
)", 0) \
    \
    DECLARE(UInt64, function_range_max_elements_in_block, 500000000, R"(
Sets the safety threshold for data volume generated by function [range](../../sql-reference/functions/array-functions.md/#range). Defines the maximum number of values generated by function per block of data (sum of array sizes for every row in a block).

Possible values:

- Positive integer.

**See Also**

- [max_block_size](#setting-max_block_size)
- [min_insert_block_size_rows](#min-insert-block-size-rows)
)", 0) \
    DECLARE(UInt64, function_sleep_max_microseconds_per_block, 3000000, R"(
Maximum number of microseconds the function `sleep` is allowed to sleep for each block. If a user called it with a larger value, it throws an exception. It is a safety threshold.
)", 0) \
    DECLARE(UInt64, function_visible_width_behavior, 1, R"(
The version of `visibleWidth` behavior. 0 - only count the number of code points; 1 - correctly count zero-width and combining characters, count full-width characters as two, estimate the tab width, count delete characters.
)", 0) \
    DECLARE(ShortCircuitFunctionEvaluation, short_circuit_function_evaluation, ShortCircuitFunctionEvaluation::ENABLE, R"(
Allows calculating the [if](../../sql-reference/functions/conditional-functions.md/#if), [multiIf](../../sql-reference/functions/conditional-functions.md/#multiif), [and](../../sql-reference/functions/logical-functions.md/#logical-and-function), and [or](../../sql-reference/functions/logical-functions.md/#logical-or-function) functions according to a [short scheme](https://en.wikipedia.org/wiki/Short-circuit_evaluation). This helps optimize the execution of complex expressions in these functions and prevent possible exceptions (such as division by zero when it is not expected).

Possible values:

- `enable` — Enables short-circuit function evaluation for functions that are suitable for it (can throw an exception or computationally heavy).
- `force_enable` — Enables short-circuit function evaluation for all functions.
- `disable` — Disables short-circuit function evaluation.
)", 0) \
    \
    DECLARE(LocalFSReadMethod, storage_file_read_method, LocalFSReadMethod::pread, R"(
Method of reading data from storage file, one of: `read`, `pread`, `mmap`. The mmap method does not apply to clickhouse-server (it's intended for clickhouse-local).
)", 0) \
    DECLARE(String, local_filesystem_read_method, "pread_threadpool", R"(
Method of reading data from local filesystem, one of: read, pread, mmap, io_uring, pread_threadpool. The 'io_uring' method is experimental and does not work for Log, TinyLog, StripeLog, File, Set and Join, and other tables with append-able files in presence of concurrent reads and writes.
)", 0) \
    DECLARE(String, remote_filesystem_read_method, "threadpool", R"(
Method of reading data from remote filesystem, one of: read, threadpool.
)", 0) \
    DECLARE(Bool, local_filesystem_read_prefetch, false, R"(
Should use prefetching when reading data from local filesystem.
)", 0) \
    DECLARE(Bool, remote_filesystem_read_prefetch, true, R"(
Should use prefetching when reading data from remote filesystem.
)", 0) \
    DECLARE(Int64, read_priority, 0, R"(
Priority to read data from local filesystem or remote filesystem. Only supported for 'pread_threadpool' method for local filesystem and for `threadpool` method for remote filesystem.
)", 0) \
    DECLARE(UInt64, merge_tree_min_rows_for_concurrent_read_for_remote_filesystem, 0, R"(
The minimum number of lines to read from one file before the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem. We do not recommend using this setting.

Possible values:

- Positive integer.
)", 0) \
    DECLARE(UInt64, merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem, 0, R"(
The minimum number of bytes to read from one file before [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem. We do not recommend using this setting.

Possible values:

- Positive integer.
)", 0) \
    DECLARE(UInt64, remote_read_min_bytes_for_seek, 4 * DBMS_DEFAULT_BUFFER_SIZE, R"(
Min bytes required for remote read (url, s3) to do seek, instead of read with ignore.
)", 0) \
    DECLARE(UInt64, merge_tree_min_bytes_per_task_for_remote_reading, 2 * DBMS_DEFAULT_BUFFER_SIZE, R"(
Min bytes to read per task.
)", 0) ALIAS(filesystem_prefetch_min_bytes_for_single_read_task) \
    DECLARE(Bool, merge_tree_use_const_size_tasks_for_remote_reading, true, R"(
Whether to use constant size tasks for reading from a remote table.
)", 0) \
    DECLARE(Bool, merge_tree_determine_task_size_by_prewhere_columns, true, R"(
Whether to use only prewhere columns size to determine reading task size.
)", 0) \
    DECLARE(UInt64, merge_tree_min_read_task_size, 8, R"(
Hard lower limit on the task size (even when the number of granules is low and the number of available threads is high we won't allocate smaller tasks
)", 0) \
    DECLARE(UInt64, merge_tree_compact_parts_min_granules_to_multibuffer_read, 16, R"(
Only available in ClickHouse Cloud. Number of granules in stripe of compact part of MergeTree tables to use multibuffer reader, which supports parallel reading and prefetch. In case of reading from remote fs using of multibuffer reader increases number of read request.
)", 0) \
    \
    DECLARE(Bool, async_insert, false, R"(
If true, data from INSERT query is stored in queue and later flushed to table in background. If wait_for_async_insert is false, INSERT query is processed almost instantly, otherwise client will wait until data will be flushed to table
)", 0) \
    DECLARE(Bool, wait_for_async_insert, true, R"(
If true wait for processing of asynchronous insertion
)", 0) \
    DECLARE(Seconds, wait_for_async_insert_timeout, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, R"(
Timeout for waiting for processing asynchronous insertion
)", 0) \
    DECLARE(UInt64, async_insert_max_data_size, 10485760, R"(
Maximum size in bytes of unparsed data collected per query before being inserted
)", 0) \
    DECLARE(UInt64, async_insert_max_query_number, 450, R"(
Maximum number of insert queries before being inserted
)", 0) \
    DECLARE(Milliseconds, async_insert_poll_timeout_ms, 10, R"(
Timeout for polling data from asynchronous insert queue
)", 0) \
    DECLARE(Bool, async_insert_use_adaptive_busy_timeout, true, R"(
If it is set to true, use adaptive busy timeout for asynchronous inserts
)", 0) \
    DECLARE(Milliseconds, async_insert_busy_timeout_min_ms, 50, R"(
If auto-adjusting is enabled through async_insert_use_adaptive_busy_timeout, minimum time to wait before dumping collected data per query since the first data appeared. It also serves as the initial value for the adaptive algorithm
)", 0) \
    DECLARE(Milliseconds, async_insert_busy_timeout_max_ms, 200, R"(
Maximum time to wait before dumping collected data per query since the first data appeared.
)", 0) ALIAS(async_insert_busy_timeout_ms) \
    DECLARE(Double, async_insert_busy_timeout_increase_rate, 0.2, R"(
The exponential growth rate at which the adaptive asynchronous insert timeout increases
)", 0) \
    DECLARE(Double, async_insert_busy_timeout_decrease_rate, 0.2, R"(
The exponential growth rate at which the adaptive asynchronous insert timeout decreases
)", 0) \
    \
    DECLARE(UInt64, remote_fs_read_max_backoff_ms, 10000, R"(
Max wait time when trying to read data for remote disk
)", 0) \
    DECLARE(UInt64, remote_fs_read_backoff_max_tries, 5, R"(
Max attempts to read with backoff
)", 0) \
    DECLARE(Bool, enable_filesystem_cache, true, R"(
Use cache for remote filesystem. This setting does not turn on/off cache for disks (must be done via disk config), but allows to bypass cache for some queries if intended
)", 0) \
    DECLARE(String, filesystem_cache_name, "", R"(
Filesystem cache name to use for stateless table engines or data lakes
)", 0) \
    DECLARE(Bool, enable_filesystem_cache_on_write_operations, false, R"(
Write into cache on write operations. To actually work this setting requires be added to disk config too
)", 0) \
    DECLARE(Bool, enable_filesystem_cache_log, false, R"(
Allows to record the filesystem caching log for each query
)", 0) \
    DECLARE(Bool, read_from_filesystem_cache_if_exists_otherwise_bypass_cache, false, R"(
Allow to use the filesystem cache in passive mode - benefit from the existing cache entries, but don't put more entries into the cache. If you set this setting for heavy ad-hoc queries and leave it disabled for short real-time queries, this will allows to avoid cache threshing by too heavy queries and to improve the overall system efficiency.
)", 0) \
    DECLARE(Bool, skip_download_if_exceeds_query_cache, true, R"(
Skip download from remote filesystem if exceeds query cache size
)", 0) \
    DECLARE(UInt64, filesystem_cache_max_download_size, (128UL * 1024 * 1024 * 1024), R"(
Max remote filesystem cache size that can be downloaded by a single query
)", 0) \
    DECLARE(Bool, throw_on_error_from_cache_on_write_operations, false, R"(
Ignore error from cache when caching on write operations (INSERT, merges)
)", 0) \
    DECLARE(UInt64, filesystem_cache_segments_batch_size, 20, R"(
Limit on size of a single batch of file segments that a read buffer can request from cache. Too low value will lead to excessive requests to cache, too large may slow down eviction from cache
)", 0) \
    DECLARE(UInt64, filesystem_cache_reserve_space_wait_lock_timeout_milliseconds, 1000, R"(
Wait time to lock cache for space reservation in filesystem cache
)", 0) \
    DECLARE(UInt64, temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds, (10 * 60 * 1000), R"(
Wait time to lock cache for space reservation for temporary data in filesystem cache
)", 0) \
    \
    DECLARE(Bool, use_page_cache_for_disks_without_file_cache, false, R"(
Use userspace page cache for remote disks that don't have filesystem cache enabled.
)", 0) \
    DECLARE(Bool, read_from_page_cache_if_exists_otherwise_bypass_cache, false, R"(
Use userspace page cache in passive mode, similar to read_from_filesystem_cache_if_exists_otherwise_bypass_cache.
)", 0) \
    DECLARE(Bool, page_cache_inject_eviction, false, R"(
Userspace page cache will sometimes invalidate some pages at random. Intended for testing.
)", 0) \
    \
    DECLARE(Bool, load_marks_asynchronously, false, R"(
Load MergeTree marks asynchronously
)", 0) \
    DECLARE(Bool, enable_filesystem_read_prefetches_log, false, R"(
Log to system.filesystem prefetch_log during query. Should be used only for testing or debugging, not recommended to be turned on by default
)", 0) \
    DECLARE(Bool, allow_prefetched_read_pool_for_remote_filesystem, true, R"(
Prefer prefetched threadpool if all parts are on remote filesystem
)", 0) \
    DECLARE(Bool, allow_prefetched_read_pool_for_local_filesystem, false, R"(
Prefer prefetched threadpool if all parts are on local filesystem
)", 0) \
    \
    DECLARE(UInt64, prefetch_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, R"(
The maximum size of the prefetch buffer to read from the filesystem.
)", 0) \
    DECLARE(UInt64, filesystem_prefetch_step_bytes, 0, R"(
Prefetch step in bytes. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task
)", 0) \
    DECLARE(UInt64, filesystem_prefetch_step_marks, 0, R"(
Prefetch step in marks. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task
)", 0) \
    DECLARE(UInt64, filesystem_prefetch_max_memory_usage, "1Gi", R"(
Maximum memory usage for prefetches.
)", 0) \
    DECLARE(UInt64, filesystem_prefetches_limit, 200, R"(
Maximum number of prefetches. Zero means unlimited. A setting `filesystem_prefetches_max_memory_usage` is more recommended if you want to limit the number of prefetches
)", 0) \
    \
    DECLARE(UInt64, use_structure_from_insertion_table_in_table_functions, 2, R"(
Use structure from insertion table instead of schema inference from data. Possible values: 0 - disabled, 1 - enabled, 2 - auto
)", 0) \
    \
    DECLARE(UInt64, http_max_tries, 10, R"(
Max attempts to read via http.
)", 0) \
    DECLARE(UInt64, http_retry_initial_backoff_ms, 100, R"(
Min milliseconds for backoff, when retrying read via http
)", 0) \
    DECLARE(UInt64, http_retry_max_backoff_ms, 10000, R"(
Max milliseconds for backoff, when retrying read via http
)", 0) \
    \
    DECLARE(Bool, force_remove_data_recursively_on_drop, false, R"(
Recursively remove data on DROP query. Avoids 'Directory not empty' error, but may silently remove detached data
)", 0) \
    DECLARE(Bool, check_table_dependencies, true, R"(
Check that DDL query (such as DROP TABLE or RENAME) will not break dependencies
)", 0) \
    DECLARE(Bool, check_referential_table_dependencies, false, R"(
Check that DDL query (such as DROP TABLE or RENAME) will not break referential dependencies
)", 0) \
    DECLARE(Bool, use_local_cache_for_remote_storage, true, R"(
Use local cache for remote storage like HDFS or S3, it's used for remote table engine only
)", 0) \
    \
    DECLARE(Bool, allow_unrestricted_reads_from_keeper, false, R"(
Allow unrestricted (without condition on path) reads from system.zookeeper table, can be handy, but is not safe for zookeeper
)", 0) \
    DECLARE(Bool, allow_deprecated_database_ordinary, false, R"(
Allow to create databases with deprecated Ordinary engine
)", 0) \
    DECLARE(Bool, allow_deprecated_syntax_for_merge_tree, false, R"(
Allow to create *MergeTree tables with deprecated engine definition syntax
)", 0) \
    DECLARE(Bool, allow_asynchronous_read_from_io_pool_for_merge_tree, false, R"(
Use background I/O pool to read from MergeTree tables. This setting may increase performance for I/O bound queries
)", 0) \
    DECLARE(UInt64, max_streams_for_merge_tree_reading, 0, R"(
If is not zero, limit the number of reading streams for MergeTree table.
)", 0) \
    \
    DECLARE(Bool, force_grouping_standard_compatibility, true, R"(
Make GROUPING function to return 1 when argument is not used as an aggregation key
)", 0) \
    \
    DECLARE(Bool, schema_inference_use_cache_for_file, true, R"(
Use cache in schema inference while using file table function
)", 0) \
    DECLARE(Bool, schema_inference_use_cache_for_s3, true, R"(
Use cache in schema inference while using s3 table function
)", 0) \
    DECLARE(Bool, schema_inference_use_cache_for_azure, true, R"(
Use cache in schema inference while using azure table function
)", 0) \
    DECLARE(Bool, schema_inference_use_cache_for_hdfs, true, R"(
Use cache in schema inference while using hdfs table function
)", 0) \
    DECLARE(Bool, schema_inference_use_cache_for_url, true, R"(
Use cache in schema inference while using url table function
)", 0) \
    DECLARE(Bool, schema_inference_cache_require_modification_time_for_url, true, R"(
Use schema from cache for URL with last modification time validation (for URLs with Last-Modified header)
)", 0) \
    \
    DECLARE(String, compatibility, "", R"(
The `compatibility` setting causes ClickHouse to use the default settings of a previous version of ClickHouse, where the previous version is provided as the setting.

If settings are set to non-default values, then those settings are honored (only settings that have not been modified are affected by the `compatibility` setting).

This setting takes a ClickHouse version number as a string, like `22.3`, `22.8`. An empty value means that this setting is disabled.

Disabled by default.

:::note
In ClickHouse Cloud the compatibility setting must be set by ClickHouse Cloud support.  Please [open a case](https://clickhouse.cloud/support) to have it set.
:::
)", 0) \
    \
    DECLARE(Map, additional_table_filters, "", R"(
An additional filter expression that is applied after reading
from the specified table.

**Example**

``` sql
INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');
SELECT * FROM table_1;
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 2 │ bb   │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
```sql
SELECT *
FROM table_1
SETTINGS additional_table_filters = {'table_1': 'x != 2'}
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
)", 0) \
    DECLARE(String, additional_result_filter, "", R"(
An additional filter expression to apply to the result of `SELECT` query.
This setting is not applied to any subquery.

**Example**

``` sql
INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');
SElECT * FROM table_1;
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 2 │ bb   │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
```sql
SELECT *
FROM table_1
SETTINGS additional_result_filter = 'x != 2'
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
)", 0) \
    \
    DECLARE(String, workload, "default", R"(
Name of workload to be used to access resources
)", 0) \
    DECLARE(Milliseconds, storage_system_stack_trace_pipe_read_timeout_ms, 100, R"(
Maximum time to read from a pipe for receiving information from the threads when querying the `system.stack_trace` table. This setting is used for testing purposes and not meant to be changed by users.
)", 0) \
    \
    DECLARE(String, rename_files_after_processing, "", R"(
- **Type:** String

- **Default value:** Empty string

This setting allows to specify renaming pattern for files processed by `file` table function. When option is set, all files read by `file` table function will be renamed according to specified pattern with placeholders, only if files processing was successful.

### Placeholders

- `%a` — Full original filename (e.g., "sample.csv").
- `%f` — Original filename without extension (e.g., "sample").
- `%e` — Original file extension with dot (e.g., ".csv").
- `%t` — Timestamp (in microseconds).
- `%%` — Percentage sign ("%").

### Example
- Option: `--rename_files_after_processing="processed_%f_%t%e"`

- Query: `SELECT * FROM file('sample.csv')`


If reading `sample.csv` is successful, file will be renamed to `processed_sample_1683473210851438.csv`
)", 0) \
    \
    /* CLOUD ONLY */ \
    DECLARE(Bool, read_through_distributed_cache, false, R"(
Only in ClickHouse Cloud. Allow reading from distributed cache
)", 0) \
    DECLARE(Bool, write_through_distributed_cache, false, R"(
Only in ClickHouse Cloud. Allow writing to distributed cache (writing to s3 will also be done by distributed cache)
)", 0) \
    DECLARE(Bool, distributed_cache_throw_on_error, false, R"(
Only in ClickHouse Cloud. Rethrow exception happened during communication with distributed cache or exception received from distributed cache. Otherwise fallback to skipping distributed cache on error
)", 0) \
    DECLARE(DistributedCacheLogMode, distributed_cache_log_mode, DistributedCacheLogMode::LOG_ON_ERROR, R"(
Only in ClickHouse Cloud. Mode for writing to system.distributed_cache_log
)", 0) \
    DECLARE(Bool, distributed_cache_fetch_metrics_only_from_current_az, true, R"(
Only in ClickHouse Cloud. Fetch metrics only from current availability zone in system.distributed_cache_metrics, system.distributed_cache_events
)", 0) \
    DECLARE(UInt64, distributed_cache_connect_max_tries, 100, R"(
Only in ClickHouse Cloud. Number of tries to connect to distributed cache if unsuccessful
)", 0) \
    DECLARE(UInt64, distributed_cache_receive_response_wait_milliseconds, 60000, R"(
Only in ClickHouse Cloud. Wait time in milliseconds to receive data for request from distributed cache
)", 0) \
    DECLARE(UInt64, distributed_cache_receive_timeout_milliseconds, 10000, R"(
Only in ClickHouse Cloud. Wait time in milliseconds to receive any kind of response from distributed cache
)", 0) \
    DECLARE(UInt64, distributed_cache_wait_connection_from_pool_milliseconds, 100, R"(
Only in ClickHouse Cloud. Wait time in milliseconds to receive connection from connection pool if distributed_cache_pool_behaviour_on_limit is wait
)", 0) \
    DECLARE(Bool, distributed_cache_bypass_connection_pool, false, R"(
Only in ClickHouse Cloud. Allow to bypass distributed cache connection pool
)", 0) \
    DECLARE(DistributedCachePoolBehaviourOnLimit, distributed_cache_pool_behaviour_on_limit, DistributedCachePoolBehaviourOnLimit::ALLOCATE_NEW_BYPASSING_POOL, R"(
Only in ClickHouse Cloud. Identifies behaviour of distributed cache connection on pool limit reached
)", 0) \
    DECLARE(UInt64, distributed_cache_read_alignment, 0, R"(
Only in ClickHouse Cloud. A setting for testing purposes, do not change it
)", 0) \
    DECLARE(UInt64, distributed_cache_max_unacked_inflight_packets, DistributedCache::MAX_UNACKED_INFLIGHT_PACKETS, R"(
Only in ClickHouse Cloud. A maximum number of unacknowledged in-flight packets in a single distributed cache read request
)", 0) \
    DECLARE(UInt64, distributed_cache_data_packet_ack_window, DistributedCache::ACK_DATA_PACKET_WINDOW, R"(
Only in ClickHouse Cloud. A window for sending ACK for DataPacket sequence in a single distributed cache read request
)", 0) \
    DECLARE(Bool, distributed_cache_discard_connection_if_unread_data, true, R"(
Only in ClickHouse Cloud. Discard connection if some data is unread.
)", 0) \
    DECLARE(Bool, filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage, true, R"(
Only in ClickHouse Cloud. Wait time to lock cache for space reservation in filesystem cache
)", 0) \
    DECLARE(Bool, filesystem_cache_enable_background_download_during_fetch, true, R"(
Only in ClickHouse Cloud. Wait time to lock cache for space reservation in filesystem cache
)", 0) \
    \
    DECLARE(Bool, parallelize_output_from_storages, true, R"(
Parallelize output for reading step from storage. It allows parallelization of  query processing right after reading from storage if possible
)", 0) \
    DECLARE(String, insert_deduplication_token, "", R"(
The setting allows a user to provide own deduplication semantic in MergeTree/ReplicatedMergeTree
For example, by providing a unique value for the setting in each INSERT statement,
user can avoid the same inserted data being deduplicated.


Possible values:

- Any string

`insert_deduplication_token` is used for deduplication _only_ when not empty.

For the replicated tables by default the only 100 of the most recent inserts for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated-deduplication-window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated-deduplication-window-seconds)).
For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non-replicated-deduplication-window).

:::note
`insert_deduplication_token` works on a partition level (the same as `insert_deduplication` checksum). Multiple partitions can have the same `insert_deduplication_token`.
:::

Example:

```sql
CREATE TABLE test_table
( A Int64 )
ENGINE = MergeTree
ORDER BY A
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (1);

-- the next insert won't be deduplicated because insert_deduplication_token is different
INSERT INTO test_table SETTINGS insert_deduplication_token = 'test1' VALUES (1);

-- the next insert will be deduplicated because insert_deduplication_token
-- is the same as one of the previous
INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (2);

SELECT * FROM test_table

┌─A─┐
│ 1 │
└───┘
┌─A─┐
│ 1 │
└───┘
```
)", 0) \
    DECLARE(Bool, count_distinct_optimization, false, R"(
Rewrite count distinct to subquery of group by
)", 0) \
    DECLARE(Bool, throw_if_no_data_to_insert, true, R"(
Allows or forbids empty INSERTs, enabled by default (throws an error on an empty insert). Only applies to INSERTs using [`clickhouse-client`](/docs/en/interfaces/cli) or using the [gRPC interface](/docs/en/interfaces/grpc).
)", 0) \
    DECLARE(Bool, compatibility_ignore_auto_increment_in_create_table, false, R"(
Ignore AUTO_INCREMENT keyword in column declaration if true, otherwise return error. It simplifies migration from MySQL
)", 0) \
    DECLARE(Bool, multiple_joins_try_to_keep_original_names, false, R"(
Do not add aliases to top level expression list on multiple joins rewrite
)", 0) \
    DECLARE(Bool, optimize_sorting_by_input_stream_properties, true, R"(
Optimize sorting by sorting properties of input stream
)", 0) \
    DECLARE(UInt64, keeper_max_retries, 10, R"(
Max retries for general keeper operations
)", 0) \
    DECLARE(UInt64, keeper_retry_initial_backoff_ms, 100, R"(
Initial backoff timeout for general keeper operations
)", 0) \
    DECLARE(UInt64, keeper_retry_max_backoff_ms, 5000, R"(
Max backoff timeout for general keeper operations
)", 0) \
    DECLARE(UInt64, insert_keeper_max_retries, 20, R"(
The setting sets the maximum number of retries for ClickHouse Keeper (or ZooKeeper) requests during insert into replicated MergeTree. Only Keeper requests which failed due to network error, Keeper session timeout, or request timeout are considered for retries.

Possible values:

- Positive integer.
- 0 — Retries are disabled

Cloud default value: `20`.

Keeper request retries are done after some timeout. The timeout is controlled by the following settings: `insert_keeper_retry_initial_backoff_ms`, `insert_keeper_retry_max_backoff_ms`.
The first retry is done after `insert_keeper_retry_initial_backoff_ms` timeout. The consequent timeouts will be calculated as follows:
```
timeout = min(insert_keeper_retry_max_backoff_ms, latest_timeout * 2)
```

For example, if `insert_keeper_retry_initial_backoff_ms=100`, `insert_keeper_retry_max_backoff_ms=10000` and `insert_keeper_max_retries=8` then timeouts will be `100, 200, 400, 800, 1600, 3200, 6400, 10000`.

Apart from fault tolerance, the retries aim to provide a better user experience - they allow to avoid returning an error during INSERT execution if Keeper is restarted, for example, due to an upgrade.
)", 0) \
    DECLARE(UInt64, insert_keeper_retry_initial_backoff_ms, 100, R"(
Initial timeout(in milliseconds) to retry a failed Keeper request during INSERT query execution

Possible values:

- Positive integer.
- 0 — No timeout
)", 0) \
    DECLARE(UInt64, insert_keeper_retry_max_backoff_ms, 10000, R"(
Maximum timeout (in milliseconds) to retry a failed Keeper request during INSERT query execution

Possible values:

- Positive integer.
- 0 — Maximum timeout is not limited
)", 0) \
    DECLARE(Float, insert_keeper_fault_injection_probability, 0.0f, R"(
Approximate probability of failure for a keeper request during insert. Valid value is in interval [0.0f, 1.0f]
)", 0) \
    DECLARE(UInt64, insert_keeper_fault_injection_seed, 0, R"(
0 - random seed, otherwise the setting value
)", 0) \
    DECLARE(Bool, force_aggregation_in_order, false, R"(
The setting is used by the server itself to support distributed queries. Do not change it manually, because it will break normal operations. (Forces use of aggregation in order on remote nodes during distributed aggregation).
)", IMPORTANT) \
    DECLARE(UInt64, http_max_request_param_data_size, 10_MiB, R"(
Limit on size of request data used as a query parameter in predefined HTTP requests.
)", 0) \
    DECLARE(Bool, function_json_value_return_type_allow_nullable, false, R"(
Control whether allow to return `NULL` when value is not exist for JSON_VALUE function.

```sql
SELECT JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true;

┌─JSON_VALUE('{"hello":"world"}', '$.b')─┐
│ ᴺᵁᴸᴸ                                   │
└────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

Possible values:

- true — Allow.
- false — Disallow.
)", 0) \
    DECLARE(Bool, function_json_value_return_type_allow_complex, false, R"(
Control whether allow to return complex type (such as: struct, array, map) for json_value function.

```sql
SELECT JSON_VALUE('{"hello":{"world":"!"}}', '$.hello') settings function_json_value_return_type_allow_complex=true

┌─JSON_VALUE('{"hello":{"world":"!"}}', '$.hello')─┐
│ {"world":"!"}                                    │
└──────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

Possible values:

- true — Allow.
- false — Disallow.
)", 0) \
    DECLARE(Bool, use_with_fill_by_sorting_prefix, true, R"(
Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently
)", 0) \
    DECLARE(Bool, optimize_uniq_to_count, true, R"(
Rewrite uniq and its variants(except uniqUpTo) to count if subquery has distinct or group by clause.
)", 0) \
    DECLARE(Bool, use_variant_as_common_type, false, R"(
Allows to use `Variant` type as a result type for [if](../../sql-reference/functions/conditional-functions.md/#if)/[multiIf](../../sql-reference/functions/conditional-functions.md/#multiif)/[array](../../sql-reference/functions/array-functions.md)/[map](../../sql-reference/functions/tuple-map-functions.md) functions when there is no common type for argument types.

Example:

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(if(number % 2, number, range(number))) as variant_type FROM numbers(1);
SELECT if(number % 2, number, range(number)) as variant FROM numbers(5);
```

```text
┌─variant_type───────────────────┐
│ Variant(Array(UInt64), UInt64) │
└────────────────────────────────┘
┌─variant───┐
│ []        │
│ 1         │
│ [0,1]     │
│ 3         │
│ [0,1,2,3] │
└───────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL)) AS variant_type FROM numbers(1);
SELECT multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL) AS variant FROM numbers(4);
```

```text
─variant_type─────────────────────────┐
│ Variant(Array(UInt8), String, UInt8) │
└──────────────────────────────────────┘

┌─variant───────┐
│ 42            │
│ [1,2,3]       │
│ Hello, World! │
│ ᴺᵁᴸᴸ          │
└───────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(array(range(number), number, 'str_' || toString(number))) as array_of_variants_type from numbers(1);
SELECT array(range(number), number, 'str_' || toString(number)) as array_of_variants FROM numbers(3);
```

```text
┌─array_of_variants_type────────────────────────┐
│ Array(Variant(Array(UInt64), String, UInt64)) │
└───────────────────────────────────────────────┘

┌─array_of_variants─┐
│ [[],0,'str_0']    │
│ [[0],1,'str_1']   │
│ [[0,1],2,'str_2'] │
└───────────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(map('a', range(number), 'b', number, 'c', 'str_' || toString(number))) as map_of_variants_type from numbers(1);
SELECT map('a', range(number), 'b', number, 'c', 'str_' || toString(number)) as map_of_variants FROM numbers(3);
```

```text
┌─map_of_variants_type────────────────────────────────┐
│ Map(String, Variant(Array(UInt64), String, UInt64)) │
└─────────────────────────────────────────────────────┘

┌─map_of_variants───────────────┐
│ {'a':[],'b':0,'c':'str_0'}    │
│ {'a':[0],'b':1,'c':'str_1'}   │
│ {'a':[0,1],'b':2,'c':'str_2'} │
└───────────────────────────────┘
```
)", 0) \
    DECLARE(Bool, enable_order_by_all, true, R"(
Enables or disables sorting with `ORDER BY ALL` syntax, see [ORDER BY](../../sql-reference/statements/select/order-by.md).

Possible values:

- 0 — Disable ORDER BY ALL.
- 1 — Enable ORDER BY ALL.

**Example**

Query:

```sql
CREATE TABLE TAB(C1 Int, C2 Int, ALL Int) ENGINE=Memory();

INSERT INTO TAB VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM TAB ORDER BY ALL; -- returns an error that ALL is ambiguous

SELECT * FROM TAB ORDER BY ALL SETTINGS enable_order_by_all = 0;
```

Result:

```text
┌─C1─┬─C2─┬─ALL─┐
│ 20 │ 20 │  10 │
│ 30 │ 10 │  20 │
│ 10 │ 20 │  30 │
└────┴────┴─────┘
```
)", 0) \
    DECLARE(Float, ignore_drop_queries_probability, 0, R"(
If enabled, server will ignore all DROP table queries with specified probability (for Memory and JOIN engines it will replcase DROP to TRUNCATE). Used for testing purposes
)", 0) \
    DECLARE(Bool, traverse_shadow_remote_data_paths, false, R"(
Traverse frozen data (shadow directory) in addition to actual table data when query system.remote_data_paths
)", 0) \
    DECLARE(Bool, geo_distance_returns_float64_on_float64_arguments, true, R"(
If all four arguments to `geoDistance`, `greatCircleDistance`, `greatCircleAngle` functions are Float64, return Float64 and use double precision for internal calculations. In previous ClickHouse versions, the functions always returned Float32.
)", 0) \
    DECLARE(Bool, allow_get_client_http_header, false, R"(
Allow to use the function `getClientHTTPHeader` which lets to obtain a value of an the current HTTP request's header. It is not enabled by default for security reasons, because some headers, such as `Cookie`, could contain sensitive info. Note that the `X-ClickHouse-*` and `Authentication` headers are always restricted and cannot be obtained with this function.
)", 0) \
    DECLARE(Bool, cast_string_to_dynamic_use_inference, false, R"(
Use types inference during String to Dynamic conversion
)", 0) \
    DECLARE(Bool, enable_blob_storage_log, true, R"(
Write information about blob storage operations to system.blob_storage_log table
)", 0) \
    DECLARE(Bool, use_json_alias_for_old_object_type, false, R"(
When enabled, `JSON` data type alias will be used to create an old [Object('json')](../../sql-reference/data-types/json.md) type instead of the new [JSON](../../sql-reference/data-types/newjson.md) type.
)", 0) \
    DECLARE(Bool, allow_create_index_without_type, false, R"(
Allow CREATE INDEX query without TYPE. Query will be ignored. Made for SQL compatibility tests.
)", 0) \
    DECLARE(Bool, create_index_ignore_unique, false, R"(
Ignore UNIQUE keyword in CREATE UNIQUE INDEX. Made for SQL compatibility tests.
)", 0) \
    DECLARE(Bool, print_pretty_type_names, true, R"(
Allows to print deep-nested type names in a pretty way with indents in `DESCRIBE` query and in `toTypeName()` function.

Example:

```sql
CREATE TABLE test (a Tuple(b String, c Tuple(d Nullable(UInt64), e Array(UInt32), f Array(Tuple(g String, h Map(String, Array(Tuple(i String, j UInt64))))), k Date), l Nullable(String))) ENGINE=Memory;
DESCRIBE TABLE test FORMAT TSVRaw SETTINGS print_pretty_type_names=1;
```

```
a   Tuple(
    b String,
    c Tuple(
        d Nullable(UInt64),
        e Array(UInt32),
        f Array(Tuple(
            g String,
            h Map(
                String,
                Array(Tuple(
                    i String,
                    j UInt64
                ))
            )
        )),
        k Date
    ),
    l Nullable(String)
)
```
)", 0) \
    DECLARE(Bool, create_table_empty_primary_key_by_default, false, R"(
Allow to create *MergeTree tables with empty primary key when ORDER BY and PRIMARY KEY not specified
)", 0) \
    DECLARE(Bool, allow_named_collection_override_by_default, true, R"(
Allow named collections' fields override by default.
)", 0) \
    DECLARE(SQLSecurityType, default_normal_view_sql_security, SQLSecurityType::INVOKER, R"(
Allows to set default `SQL SECURITY` option while creating a normal view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `INVOKER`.
)", 0) \
    DECLARE(SQLSecurityType, default_materialized_view_sql_security, SQLSecurityType::DEFINER, R"(
Allows to set a default value for SQL SECURITY option when creating a materialized view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `DEFINER`.
)", 0) \
    DECLARE(String, default_view_definer, "CURRENT_USER", R"(
Allows to set default `DEFINER` option while creating a view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `CURRENT_USER`.
)", 0) \
    DECLARE(UInt64, cache_warmer_threads, 4, R"(
Only available in ClickHouse Cloud. Number of background threads for speculatively downloading new data parts into file cache, when cache_populated_by_fetch is enabled. Zero to disable.
)", 0) \
    DECLARE(Int64, ignore_cold_parts_seconds, 0, R"(
Only available in ClickHouse Cloud. Exclude new data parts from SELECT queries until they're either pre-warmed (see cache_populated_by_fetch) or this many seconds old. Only for Replicated-/SharedMergeTree.
)", 0) \
    DECLARE(Int64, prefer_warmed_unmerged_parts_seconds, 0, R"(
Only available in ClickHouse Cloud. If a merged part is less than this many seconds old and is not pre-warmed (see cache_populated_by_fetch), but all its source parts are available and pre-warmed, SELECT queries will read from those parts instead. Only for ReplicatedMergeTree. Note that this only checks whether CacheWarmer processed the part; if the part was fetched into cache by something else, it'll still be considered cold until CacheWarmer gets to it; if it was warmed, then evicted from cache, it'll still be considered warm.
)", 0) \
    DECLARE(Bool, iceberg_engine_ignore_schema_evolution, false, R"(
Allow to ignore schema evolution in Iceberg table engine and read all data using schema specified by the user on table creation or latest schema parsed from metadata on table creation.

:::note
Enabling this setting can lead to incorrect result as in case of evolved schema all data files will be read using the same schema.
:::
)", 0) \
    DECLARE(Bool, allow_deprecated_error_prone_window_functions, false, R"(
Allow usage of deprecated error prone window functions (neighbor, runningAccumulate, runningDifferenceStartingWithFirstValue, runningDifference)
)", 0) \
    DECLARE(Bool, allow_deprecated_snowflake_conversion_functions, false, R"(
Functions `snowflakeToDateTime`, `snowflakeToDateTime64`, `dateTimeToSnowflake`, and `dateTime64ToSnowflake` are deprecated and disabled by default.
Please use functions `snowflakeIDToDateTime`, `snowflakeIDToDateTime64`, `dateTimeToSnowflakeID`, and `dateTime64ToSnowflakeID` instead.

To re-enable the deprecated functions (e.g., during a transition period), please set this setting to `true`.
)", 0) \
    DECLARE(Bool, optimize_distinct_in_order, true, R"(
Enable DISTINCT optimization if some columns in DISTINCT form a prefix of sorting. For example, prefix of sorting key in merge tree or ORDER BY statement
)", 0) \
    DECLARE(Bool, keeper_map_strict_mode, false, R"(
Enforce additional checks during operations on KeeperMap. E.g. throw an exception on an insert for already existing key
)", 0) \
    DECLARE(UInt64, extract_key_value_pairs_max_pairs_per_row, 1000, R"(
Max number of pairs that can be produced by the `extractKeyValuePairs` function. Used as a safeguard against consuming too much memory.
)", 0) ALIAS(extract_kvp_max_pairs_per_row) \
    DECLARE(Bool, restore_replace_external_engines_to_null, false, R"(
For testing purposes. Replaces all external engines to Null to not initiate external connections.
)", 0) \
    DECLARE(Bool, restore_replace_external_table_functions_to_null, false, R"(
For testing purposes. Replaces all external table functions to Null to not initiate external connections.
)", 0) \
    DECLARE(Bool, restore_replace_external_dictionary_source_to_null, false, R"(
Replace external dictionary sources to Null on restore. Useful for testing purposes
)", 0) \
        /* Parallel replicas */ \
    DECLARE(UInt64, allow_experimental_parallel_reading_from_replicas, 0, R"(
Use up to `max_parallel_replicas` the number of replicas from each shard for SELECT query execution. Reading is parallelized and coordinated dynamically. 0 - disabled, 1 - enabled, silently disable them in case of failure, 2 - enabled, throw an exception in case of failure
)", BETA) ALIAS(enable_parallel_replicas) \
    DECLARE(NonZeroUInt64, max_parallel_replicas, 1, R"(
The maximum number of replicas for each shard when executing a query.

Possible values:

- Positive integer.

**Additional Info**

This options will produce different results depending on the settings used.

:::note
This setting will produce incorrect results when joins or subqueries are involved, and all tables don't meet certain requirements. See [Distributed Subqueries and max_parallel_replicas](../../sql-reference/operators/in.md/#max_parallel_replica-subqueries) for more details.
:::

### Parallel processing using `SAMPLE` key

A query may be processed faster if it is executed on several servers in parallel. But the query performance may degrade in the following cases:

- The position of the sampling key in the partitioning key does not allow efficient range scans.
- Adding a sampling key to the table makes filtering by other columns less efficient.
- The sampling key is an expression that is expensive to calculate.
- The cluster latency distribution has a long tail, so that querying more servers increases the query overall latency.

### Parallel processing using [parallel_replicas_custom_key](#parallel_replicas_custom_key)

This setting is useful for any replicated table.
)", 0) \
    DECLARE(ParallelReplicasMode, parallel_replicas_mode, ParallelReplicasMode::READ_TASKS, R"(
Type of filter to use with custom key for parallel replicas. default - use modulo operation on the custom key, range - use range filter on custom key using all possible values for the value type of custom key.
)", BETA) \
    DECLARE(UInt64, parallel_replicas_count, 0, R"(
This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the number of parallel replicas participating in query processing.
)", BETA) \
    DECLARE(UInt64, parallel_replica_offset, 0, R"(
This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the index of the replica participating in query processing among parallel replicas.
)", BETA) \
    DECLARE(String, parallel_replicas_custom_key, "", R"(
An arbitrary integer expression that can be used to split work between replicas for a specific table.
The value can be any integer expression.

Simple expressions using primary keys are preferred.

If the setting is used on a cluster that consists of a single shard with multiple replicas, those replicas will be converted into virtual shards.
Otherwise, it will behave same as for `SAMPLE` key, it will use multiple replicas of each shard.
)", BETA) \
    DECLARE(UInt64, parallel_replicas_custom_key_range_lower, 0, R"(
Allows the filter type `range` to split the work evenly between replicas based on the custom range `[parallel_replicas_custom_key_range_lower, INT_MAX]`.

When used in conjunction with [parallel_replicas_custom_key_range_upper](#parallel_replicas_custom_key_range_upper), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`.

Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing.
)", BETA) \
    DECLARE(UInt64, parallel_replicas_custom_key_range_upper, 0, R"(
Allows the filter type `range` to split the work evenly between replicas based on the custom range `[0, parallel_replicas_custom_key_range_upper]`. A value of 0 disables the upper bound, setting it the max value of the custom key expression.

When used in conjunction with [parallel_replicas_custom_key_range_lower](#parallel_replicas_custom_key_range_lower), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`.

Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing
)", BETA) \
    DECLARE(String, cluster_for_parallel_replicas, "", R"(
Cluster for a shard in which current server is located
)", BETA) \
    DECLARE(Bool, parallel_replicas_allow_in_with_subquery, true, R"(
If true, subquery for IN will be executed on every follower replica.
)", BETA) \
    DECLARE(Float, parallel_replicas_single_task_marks_count_multiplier, 2, R"(
A multiplier which will be added during calculation for minimal number of marks to retrieve from coordinator. This will be applied only for remote replicas.
)", BETA) \
    DECLARE(Bool, parallel_replicas_for_non_replicated_merge_tree, false, R"(
If true, ClickHouse will use parallel replicas algorithm also for non-replicated MergeTree tables
)", BETA) \
    DECLARE(UInt64, parallel_replicas_min_number_of_rows_per_replica, 0, R"(
Limit the number of replicas used in a query to (estimated rows to read / min_number_of_rows_per_replica). The max is still limited by 'max_parallel_replicas'
)", BETA) \
    DECLARE(Bool, parallel_replicas_prefer_local_join, true, R"(
If true, and JOIN can be executed with parallel replicas algorithm, and all storages of right JOIN part are *MergeTree, local JOIN will be used instead of GLOBAL JOIN.
)", BETA) \
    DECLARE(UInt64, parallel_replicas_mark_segment_size, 0, R"(
Parts virtually divided into segments to be distributed between replicas for parallel reading. This setting controls the size of these segments. Not recommended to change until you're absolutely sure in what you're doing. Value should be in range [128; 16384]
)", BETA) \
    DECLARE(Bool, parallel_replicas_local_plan, true, R"(
Build local plan for local replica
)", BETA) \
    \
    DECLARE(Bool, allow_experimental_analyzer, true, R"(
Allow new query analyzer.
)", IMPORTANT | BETA) ALIAS(enable_analyzer) \
    DECLARE(Bool, analyzer_compatibility_join_using_top_level_identifier, false, R"(
Force to resolve identifier in JOIN USING from projection (for example, in `SELECT a + 1 AS b FROM t1 JOIN t2 USING (b)` join will be performed by `t1.a + 1 = t2.b`, rather then `t1.b = t2.b`).
)", BETA) \
    \
    DECLARE(Timezone, session_timezone, "", R"(
Sets the implicit time zone of the current session or query.
The implicit time zone is the time zone applied to values of type DateTime/DateTime64 which have no explicitly specified time zone.
The setting takes precedence over the globally configured (server-level) implicit time zone.
A value of '' (empty string) means that the implicit time zone of the current session or query is equal to the [server time zone](../server-configuration-parameters/settings.md#timezone).

You can use functions `timeZone()` and `serverTimeZone()` to get the session time zone and server time zone.

Possible values:

-    Any time zone name from `system.time_zones`, e.g. `Europe/Berlin`, `UTC` or `Zulu`

Examples:

```sql
SELECT timeZone(), serverTimeZone() FORMAT CSV

"Europe/Berlin","Europe/Berlin"
```

```sql
SELECT timeZone(), serverTimeZone() SETTINGS session_timezone = 'Asia/Novosibirsk' FORMAT CSV

"Asia/Novosibirsk","Europe/Berlin"
```

Assign session time zone 'America/Denver' to the inner DateTime without explicitly specified time zone:

```sql
SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS session_timezone = 'America/Denver' FORMAT TSV

1999-12-13 07:23:23.123
```

:::warning
Not all functions that parse DateTime/DateTime64 respect `session_timezone`. This can lead to subtle errors.
See the following example and explanation.
:::

```sql
CREATE TABLE test_tz (`d` DateTime('UTC')) ENGINE = Memory AS SELECT toDateTime('2000-01-01 00:00:00', 'UTC');

SELECT *, timeZone() FROM test_tz WHERE d = toDateTime('2000-01-01 00:00:00') SETTINGS session_timezone = 'Asia/Novosibirsk'
0 rows in set.

SELECT *, timeZone() FROM test_tz WHERE d = '2000-01-01 00:00:00' SETTINGS session_timezone = 'Asia/Novosibirsk'
┌───────────────────d─┬─timeZone()───────┐
│ 2000-01-01 00:00:00 │ Asia/Novosibirsk │
└─────────────────────┴──────────────────┘
```

This happens due to different parsing pipelines:

- `toDateTime()` without explicitly given time zone used in the first `SELECT` query honors setting `session_timezone` and the global time zone.
- In the second query, a DateTime is parsed from a String, and inherits the type and time zone of the existing column`d`. Thus, setting `session_timezone` and the global time zone are not honored.

**See also**

- [timezone](../server-configuration-parameters/settings.md#timezone)
)", BETA) \
DECLARE(Bool, create_if_not_exists, false, R"(
Enable `IF NOT EXISTS` for `CREATE` statement by default. If either this setting or `IF NOT EXISTS` is specified and a table with the provided name already exists, no exception will be thrown.
)", 0) \
    DECLARE(Bool, enforce_strict_identifier_format, false, R"(
If enabled, only allow identifiers containing alphanumeric characters and underscores.
)", 0) \
    DECLARE(Bool, mongodb_throw_on_unsupported_query, true, R"(
If enabled, MongoDB tables will return an error when a MongoDB query cannot be built. Otherwise, ClickHouse reads the full table and processes it locally. This option does not apply to the legacy implementation or when 'allow_experimental_analyzer=0'.
)", 0) \
    DECLARE(Bool, implicit_select, false, R"(
Allow writing simple SELECT queries without the leading SELECT keyword, which makes it simple for calculator-style usage, e.g. `1 + 2` becomes a valid query.
)", 0) \
    \
    \
    /* ####################################################### */ \
    /* ########### START OF EXPERIMENTAL FEATURES ############ */ \
    /* ## ADD PRODUCTION / BETA FEATURES BEFORE THIS BLOCK  ## */ \
    /* ####################################################### */ \
    \
    DECLARE(Bool, allow_experimental_materialized_postgresql_table, false, R"(
Allows to use the MaterializedPostgreSQL table engine. Disabled by default, because this feature is experimental
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_funnel_functions, false, R"(
Enable experimental functions for funnel analysis.
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_nlp_functions, false, R"(
Enable experimental functions for natural language processing.
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_hash_functions, false, R"(
Enable experimental hash functions
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_object_type, false, R"(
Allow Object and JSON data types
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_time_series_table, false, R"(
Allows creation of tables with the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine.

Possible values:

- 0 — the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine is disabled.
- 1 — the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine is enabled.
)", 0) \
    DECLARE(Bool, allow_experimental_vector_similarity_index, false, R"(
Allow experimental vector similarity index
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_variant_type, false, R"(
Allows creation of experimental [Variant](../../sql-reference/data-types/variant.md).
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_dynamic_type, false, R"(
Allow Dynamic data type
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_json_type, false, R"(
Allow JSON data type
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_codecs, false, R"(
If it is set to true, allow to specify experimental compression codecs (but we don't have those yet and this option does nothing).
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_shared_set_join, true, R"(
Only in ClickHouse Cloud. Allow to create ShareSet and SharedJoin
)", EXPERIMENTAL) \
    DECLARE(UInt64, max_limit_for_ann_queries, 1'000'000, R"(
SELECT queries with LIMIT bigger than this setting cannot use vector similarity indexes. Helps to prevent memory overflows in vector similarity indexes.
)", EXPERIMENTAL) \
    DECLARE(UInt64, hnsw_candidate_list_size_for_search, 256, R"(
The size of the dynamic candidate list when searching the vector similarity index, also known as 'ef_search'.
)", EXPERIMENTAL) \
    DECLARE(Bool, throw_on_unsupported_query_inside_transaction, true, R"(
Throw exception if unsupported query is used inside transaction
)", EXPERIMENTAL) \
    DECLARE(TransactionsWaitCSNMode, wait_changes_become_visible_after_commit_mode, TransactionsWaitCSNMode::WAIT_UNKNOWN, R"(
Wait for committed changes to become actually visible in the latest snapshot
)", EXPERIMENTAL) \
    DECLARE(Bool, implicit_transaction, false, R"(
If enabled and not already inside a transaction, wraps the query inside a full transaction (begin + commit or rollback)
)", EXPERIMENTAL) \
    DECLARE(UInt64, grace_hash_join_initial_buckets, 1, R"(
Initial number of grace hash join buckets
)", EXPERIMENTAL) \
    DECLARE(UInt64, grace_hash_join_max_buckets, 1024, R"(
Limit on the number of grace hash join buckets
)", EXPERIMENTAL) \
    DECLARE(UInt64, join_to_sort_minimum_perkey_rows, 40, R"(
The lower limit of per-key average rows in the right table to determine whether to rerange the right table by key in left or inner join. This setting ensures that the optimization is not applied for sparse table keys
)", EXPERIMENTAL) \
    DECLARE(UInt64, join_to_sort_maximum_table_rows, 10000, R"(
The maximum number of rows in the right table to determine whether to rerange the right table by key in left or inner join.
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_join_right_table_sorting, false, R"(
If it is set to true, and the conditions of `join_to_sort_minimum_perkey_rows` and `join_to_sort_maximum_table_rows` are met, rerange the right table by key to improve the performance in left or inner hash join.
)", EXPERIMENTAL) \
    DECLARE(Bool, use_hive_partitioning, false, R"(
When enabled, ClickHouse will detect Hive-style partitioning in path (`/name=value/`) in file-like table engines [File](../../engines/table-engines/special/file.md#hive-style-partitioning)/[S3](../../engines/table-engines/integrations/s3.md#hive-style-partitioning)/[URL](../../engines/table-engines/special/url.md#hive-style-partitioning)/[HDFS](../../engines/table-engines/integrations/hdfs.md#hive-style-partitioning)/[AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md#hive-style-partitioning) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.
)", EXPERIMENTAL)\
    \
    DECLARE(Bool, allow_statistics_optimize, false, R"(
Allows using statistics to optimize queries
)", EXPERIMENTAL) ALIAS(allow_statistic_optimize) \
    DECLARE(Bool, allow_experimental_statistics, false, R"(
Allows defining columns with [statistics](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) and [manipulate statistics](../../engines/table-engines/mergetree-family/mergetree.md#column-statistics).
)", EXPERIMENTAL) ALIAS(allow_experimental_statistic) \
    \
    DECLARE(Bool, allow_archive_path_syntax, true, R"(
File/S3 engines/table function will parse paths with '::' as '\\<archive\\> :: \\<file\\>' if archive has correct extension
)", EXPERIMENTAL) \
    \
    DECLARE(Bool, allow_experimental_inverted_index, false, R"(
If it is set to true, allow to use experimental inverted index.
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_full_text_index, false, R"(
If it is set to true, allow to use experimental full-text index.
)", EXPERIMENTAL) \
    \
    DECLARE(Bool, allow_experimental_join_condition, false, R"(
Support join with inequal conditions which involve columns from both left and right table. e.g. t1.y < t2.y.
)", 0) \
    \
    DECLARE(Bool, allow_experimental_live_view, false, R"(
Allows creation of a deprecated LIVE VIEW.

Possible values:

- 0 — Working with live views is disabled.
- 1 — Working with live views is enabled.
)", 0) \
    DECLARE(Seconds, live_view_heartbeat_interval, 15, R"(
The heartbeat interval in seconds to indicate live query is alive.
)", EXPERIMENTAL) \
    DECLARE(UInt64, max_live_view_insert_blocks_before_refresh, 64, R"(
Limit maximum number of inserted blocks after which mergeable blocks are dropped and query is re-executed.
)", EXPERIMENTAL) \
    \
    DECLARE(Bool, allow_experimental_window_view, false, R"(
Enable WINDOW VIEW. Not mature enough.
)", EXPERIMENTAL) \
    DECLARE(Seconds, window_view_clean_interval, 60, R"(
The clean interval of window view in seconds to free outdated data.
)", EXPERIMENTAL) \
    DECLARE(Seconds, window_view_heartbeat_interval, 15, R"(
The heartbeat interval in seconds to indicate watch query is alive.
)", EXPERIMENTAL) \
    DECLARE(Seconds, wait_for_window_view_fire_signal_timeout, 10, R"(
Timeout for waiting for window view fire signal in event time processing
)", EXPERIMENTAL) \
    \
    DECLARE(Bool, stop_refreshable_materialized_views_on_startup, false, R"(
On server startup, prevent scheduling of refreshable materialized views, as if with SYSTEM STOP VIEWS. You can manually start them with SYSTEM START VIEWS or SYSTEM START VIEW \\<name\\> afterwards. Also applies to newly created views. Has no effect on non-refreshable materialized views.
)", EXPERIMENTAL) \
    \
    DECLARE(Bool, allow_experimental_database_materialized_mysql, false, R"(
Allow to create database with Engine=MaterializedMySQL(...).
)", EXPERIMENTAL) \
    DECLARE(Bool, allow_experimental_database_materialized_postgresql, false, R"(
Allow to create database with Engine=MaterializedPostgreSQL(...).
)", EXPERIMENTAL) \
    \
    /** Experimental feature for moving data between shards. */ \
    DECLARE(Bool, allow_experimental_query_deduplication, false, R"(
Experimental data deduplication for SELECT queries based on part UUIDs
)", EXPERIMENTAL) \
    \
    /* ####################################################### */ \
    /* ############ END OF EXPERIMENTAL FEATURES ############# */ \
    /* ####################################################### */ \

// End of COMMON_SETTINGS
// Please add settings related to formats in Core/FormatFactorySettings.h, move obsolete settings to OBSOLETE_SETTINGS and obsolete format settings to OBSOLETE_FORMAT_SETTINGS.

#define OBSOLETE_SETTINGS(M, ALIAS) \
    /** Obsolete settings that do nothing but left for compatibility reasons. Remove each one after half a year of obsolescence. */ \
    MAKE_OBSOLETE(M, Bool, update_insert_deduplication_token_in_dependent_materialized_views, 0) \
    MAKE_OBSOLETE(M, UInt64, max_memory_usage_for_all_queries, 0) \
    MAKE_OBSOLETE(M, UInt64, multiple_joins_rewriter_version, 0) \
    MAKE_OBSOLETE(M, Bool, enable_debug_queries, false) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_database_atomic, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_bigint_types, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_window_functions, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_geo_types, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_query_cache, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_alter_materialized_view_structure, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_shared_merge_tree, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_database_replicated, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_refreshable_materialized_view, true) \
    \
    MAKE_OBSOLETE(M, Milliseconds, async_insert_stale_timeout_ms, 0) \
    MAKE_OBSOLETE(M, StreamingHandleErrorMode, handle_kafka_error_mode, StreamingHandleErrorMode::DEFAULT) \
    MAKE_OBSOLETE(M, Bool, database_replicated_ddl_output, true) \
    MAKE_OBSOLETE(M, UInt64, replication_alter_columns_timeout, 60) \
    MAKE_OBSOLETE(M, UInt64, odbc_max_field_size, 0) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_map_type, true) \
    MAKE_OBSOLETE(M, UInt64, merge_tree_clear_old_temporary_directories_interval_seconds, 60) \
    MAKE_OBSOLETE(M, UInt64, merge_tree_clear_old_parts_interval_seconds, 1) \
    MAKE_OBSOLETE(M, UInt64, partial_merge_join_optimizations, 0) \
    MAKE_OBSOLETE(M, MaxThreads, max_alter_threads, 0) \
    MAKE_OBSOLETE(M, Bool, use_mysql_types_in_show_columns, false) \
    MAKE_OBSOLETE(M, Bool, s3queue_allow_experimental_sharded_mode, false) \
    MAKE_OBSOLETE(M, LightweightMutationProjectionMode, lightweight_mutation_projection_mode, LightweightMutationProjectionMode::THROW) \
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
    MAKE_DEPRECATED_BY_SERVER_CONFIG(M, UInt64, max_entries_for_hash_table_stats, 10'000) \
    /* ---- */ \
    MAKE_OBSOLETE(M, DefaultDatabaseEngine, default_database_engine, DefaultDatabaseEngine::Atomic) \
    MAKE_OBSOLETE(M, UInt64, max_pipeline_depth, 0) \
    MAKE_OBSOLETE(M, Seconds, temporary_live_view_timeout, 1) \
    MAKE_OBSOLETE(M, Milliseconds, async_insert_cleanup_timeout_ms, 1000) \
    MAKE_OBSOLETE(M, Bool, optimize_fuse_sum_count_avg, 0) \
    MAKE_OBSOLETE(M, Seconds, drain_timeout, 3) \
    MAKE_OBSOLETE(M, UInt64, backup_threads, 16) \
    MAKE_OBSOLETE(M, UInt64, restore_threads, 16) \
    MAKE_OBSOLETE(M, Bool, optimize_duplicate_order_by_and_distinct, false) \
    MAKE_OBSOLETE(M, UInt64, parallel_replicas_min_number_of_granules_to_enable, 0) \
    MAKE_OBSOLETE(M, ParallelReplicasCustomKeyFilterType, parallel_replicas_custom_key_filter_type, ParallelReplicasCustomKeyFilterType::DEFAULT) \
    MAKE_OBSOLETE(M, Bool, query_plan_optimize_projection, true) \
    MAKE_OBSOLETE(M, Bool, query_cache_store_results_of_queries_with_nondeterministic_functions, false) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_annoy_index, false) \
    MAKE_OBSOLETE(M, UInt64, max_threads_for_annoy_index_creation, 4) \
    MAKE_OBSOLETE(M, Int64, annoy_index_search_k_nodes, -1) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_usearch_index, false) \
    MAKE_OBSOLETE(M, Bool, optimize_move_functions_out_of_any, false) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_undrop_table_query, true) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_s3queue, true) \
    MAKE_OBSOLETE(M, Bool, query_plan_optimize_primary_key, true) \
    MAKE_OBSOLETE(M, Bool, optimize_monotonous_functions_in_order_by, false) \
    MAKE_OBSOLETE(M, UInt64, http_max_chunk_size, 100_GiB) \
    MAKE_OBSOLETE(M, Bool, enable_deflate_qpl_codec, false) \

    /** The section above is for obsolete settings. Do not add anything there. */
#endif /// __CLION_IDE__

#define LIST_OF_SETTINGS(M, ALIAS)     \
    COMMON_SETTINGS(M, ALIAS)          \
    OBSOLETE_SETTINGS(M, ALIAS)        \
    FORMAT_FACTORY_SETTINGS(M, ALIAS)  \
    OBSOLETE_FORMAT_SETTINGS(M, ALIAS) \

// clang-format on

DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(SettingsTraits, LIST_OF_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(SettingsTraits, LIST_OF_SETTINGS)

/** Settings of query execution.
  * These settings go to users.xml.
  */
struct SettingsImpl : public BaseSettings<SettingsTraits>, public IHints<2>
{
    SettingsImpl() = default;

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

private:
    void applyCompatibilitySetting(const String & compatibility);

    std::unordered_set<std::string_view> settings_changed_by_compatibility_setting;
};

/** Set the settings from the profile (in the server configuration, many settings can be listed in one profile).
    * The profile can also be set using the `set` functions, like the `profile` setting.
    */
void SettingsImpl::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String elem = "profiles." + profile_name;

    if (!config.has(elem))
        throw Exception(ErrorCodes::THERE_IS_NO_PROFILE, "There is no profile '{}' in configuration file.", profile_name);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(elem, config_keys);

    for (const std::string & key : config_keys)
    {
        if (key == "constraints")
            continue;
        if (key == "profile" || key.starts_with("profile["))   /// Inheritance of profiles from the current one.
            setProfile(config.getString(elem + "." + key), config);
        else
            set(key, config.getString(elem + "." + key));
    }
}

void SettingsImpl::loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(path))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no path '{}' in configuration file.", path);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(path, config_keys);

    for (const std::string & key : config_keys)
    {
        set(key, config.getString(path + "." + key));
    }
}

void SettingsImpl::dumpToMapColumn(IColumn * column, bool changed_only)
{
    /// Convert ptr and make simple check
    auto * column_map = column ? &typeid_cast<ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getNestedColumn().getOffsets();
    auto & tuple_column = column_map->getNestedData();
    auto & key_column = tuple_column.getColumn(0);
    auto & value_column = tuple_column.getColumn(1);

    size_t size = 0;
    for (const auto & setting : all(changed_only ? SKIP_UNCHANGED : SKIP_NONE))
    {
        auto name = setting.getName();
        key_column.insertData(name.data(), name.size());
        value_column.insert(setting.getValueString());
        size++;
    }

    offsets.push_back(offsets.back() + size);
}

void SettingsImpl::checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path)
{
    if (config.getBool("skip_check_for_incorrect_settings", false))
        return;

    SettingsImpl settings;
    for (const auto & setting : settings.all())
    {
        const auto & name = setting.getName();
        bool should_skip_check = name == "max_table_size_to_drop" || name == "max_partition_size_to_drop";
        if (config.has(name) && (setting.getTier() != SettingsTierType::OBSOLETE) && !should_skip_check)
        {
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "A setting '{}' appeared at top level in config {}."
                " But it is user-level setting that should be located in users.xml inside <profiles> section for specific profile."
                " You can add it to <profiles><default> if you want to change default value of this setting."
                " You can also disable the check - specify <skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>"
                " in the main configuration file.",
                name, config_path);
        }
    }
}

std::vector<String> SettingsImpl::getAllRegisteredNames() const
{
    std::vector<String> all_settings;
    for (const auto & setting_field : all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}

void SettingsImpl::set(std::string_view name, const Field & value)
{
    if (name == "compatibility")
    {
        if (value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type of value for setting 'compatibility'. Expected String, got {}", value.getTypeName());
        applyCompatibilitySetting(value.safeGet<String>());
    }
    /// If we change setting that was changed by compatibility setting before
    /// we should remove it from settings_changed_by_compatibility_setting,
    /// otherwise the next time we will change compatibility setting
    /// this setting will be changed too (and we don't want it).
    else if (settings_changed_by_compatibility_setting.contains(name))
        settings_changed_by_compatibility_setting.erase(name);

    BaseSettings::set(name, value);
}

void SettingsImpl::applyCompatibilitySetting(const String & compatibility_value)
{
    /// First, revert all changes applied by previous compatibility setting
    for (const auto & setting_name : settings_changed_by_compatibility_setting)
        resetToDefault(setting_name);

    settings_changed_by_compatibility_setting.clear();
    /// If setting value is empty, we don't need to change settings
    if (compatibility_value.empty())
        return;

    ClickHouseVersion version(compatibility_value);
    const auto & settings_changes_history = getSettingsChangesHistory();
    /// Iterate through ClickHouse version in descending order and apply reversed
    /// changes for each version that is higher that version from compatibility setting
    for (auto it = settings_changes_history.rbegin(); it != settings_changes_history.rend(); ++it)
    {
        if (version >= it->first)
            break;

        /// Apply reversed changes from this version.
        for (const auto & change : it->second)
        {
            /// In case the alias is being used (e.g. use enable_analyzer) we must change the original setting
            auto final_name = SettingsTraits::resolveName(change.name);

            /// If this setting was changed manually, we don't change it
            if (isChanged(final_name) && !settings_changed_by_compatibility_setting.contains(final_name))
                continue;

            BaseSettings::set(final_name, change.previous_value);
            settings_changed_by_compatibility_setting.insert(final_name);
        }
    }
}

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    Settings ## TYPE NAME = & SettingsImpl :: NAME;

namespace Setting
{
    LIST_OF_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

Settings::Settings()
    : impl(std::make_unique<SettingsImpl>())
{}

Settings::Settings(const Settings & settings)
    : impl(std::make_unique<SettingsImpl>(*settings.impl))
{}

Settings::Settings(Settings && settings) noexcept
    : impl(std::make_unique<SettingsImpl>(std::move(*settings.impl)))
{}

Settings::~Settings() = default;

Settings & Settings::operator=(const Settings & other)
{
    *impl = *other.impl;
    return *this;
}

bool Settings::operator==(const Settings & other) const
{
    return *impl == *other.impl;
}

COMMON_SETTINGS_SUPPORTED_TYPES(Settings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

bool Settings::has(std::string_view name) const
{
    return impl->has(name);
}

bool Settings::isChanged(std::string_view name) const
{
    return impl->isChanged(name);
}

bool Settings::tryGet(std::string_view name, Field & value) const
{
    return impl->tryGet(name, value);
}

Field Settings::get(std::string_view name) const
{
    return impl->get(name);
}

void Settings::set(std::string_view name, const Field & value)
{
    impl->set(name, value);
}

void Settings::setDefaultValue(std::string_view name)
{
    impl->resetToDefault(name);
}

std::vector<String> Settings::getHints(const String & name) const
{
    return impl->getHints(name);
}

String Settings::toString() const
{
    return impl->toString();
}

SettingsChanges Settings::changes() const
{
    return impl->changes();
}

void Settings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

std::vector<std::string_view> Settings::getAllRegisteredNames() const
{
    std::vector<std::string_view> setting_names;
    for (const auto & setting : impl->all())
    {
        setting_names.emplace_back(setting.getName());
    }
    return setting_names;
}

std::vector<std::string_view> Settings::getChangedAndObsoleteNames() const
{
    std::vector<std::string_view> setting_names;
    for (const auto & setting : impl->allChanged())
    {
        if (setting.getTier() == SettingsTierType::OBSOLETE)
            setting_names.emplace_back(setting.getName());
    }
    return setting_names;
}

std::vector<std::string_view> Settings::getUnchangedNames() const
{
    std::vector<std::string_view> setting_names;
    for (const auto & setting : impl->allUnchanged())
    {
        setting_names.emplace_back(setting.getName());
    }
    return setting_names;
}

void Settings::dumpToSystemSettingsColumns(MutableColumnsAndConstraints & params) const
{
    MutableColumns & res_columns = params.res_columns;

    const auto fill_data_for_setting = [&](std::string_view setting_name, const auto & setting)
    {
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());

        /// Trim starting/ending newline.
        std::string_view doc = setting.getDescription();
        if (!doc.empty() && doc[0] == '\n')
            doc = doc.substr(1);
        if (!doc.empty() && doc[doc.length() - 1] == '\n')
            doc = doc.substr(0, doc.length() - 1);

        res_columns[3]->insert(doc);

        Field min, max;
        SettingConstraintWritability writability = SettingConstraintWritability::WRITABLE;
        params.constraints.get(*this, setting_name, min, max, writability);

        /// These two columns can accept strings only.
        if (!min.isNull())
            min = Settings::valueToStringUtil(setting_name, min);
        if (!max.isNull())
            max = Settings::valueToStringUtil(setting_name, max);

        res_columns[4]->insert(min);
        res_columns[5]->insert(max);
        res_columns[6]->insert(writability == SettingConstraintWritability::CONST);
        res_columns[7]->insert(setting.getTypeName());
        res_columns[8]->insert(setting.getDefaultValueString());
        res_columns[10]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
        res_columns[11]->insert(setting.getTier());
    };

    const auto & settings_to_aliases = SettingsImpl::Traits::settingsToAliases();
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);

        fill_data_for_setting(setting_name, setting);
        res_columns[9]->insert("");

        if (auto it = settings_to_aliases.find(setting_name); it != settings_to_aliases.end())
        {
            for (const auto alias : it->second)
            {
                res_columns[0]->insert(alias);
                fill_data_for_setting(alias, setting);
                res_columns[9]->insert(setting_name);
            }
        }
    }
}

void Settings::dumpToMapColumn(IColumn * column, bool changed_only) const
{
    impl->dumpToMapColumn(column, changed_only);
}

NameToNameMap Settings::toNameToNameMap() const
{
    NameToNameMap query_parameters;
    for (const auto & param : *impl)
    {
        std::string value;
        ReadBufferFromOwnString buf(param.getValueString());
        readQuoted(value, buf);
        query_parameters.emplace(param.getName(), value);
    }
    return query_parameters;
}

void Settings::write(WriteBuffer & out, SettingsWriteFormat format) const
{
    impl->write(out, format);
}

void Settings::read(ReadBuffer & in, SettingsWriteFormat format)
{
    impl->read(in, format);
}

void Settings::addToProgramOptions(boost::program_options::options_description & options)
{
    addProgramOptions(*impl, options);
}

void Settings::addToProgramOptions(std::string_view setting_name, boost::program_options::options_description & options)
{
    const auto & accessor = SettingsImpl::Traits::Accessor::instance();
    size_t index = accessor.find(setting_name);
    chassert(index != static_cast<size_t>(-1));
    auto on_program_option = boost::function1<void, const std::string &>(
            [this, setting_name](const std::string & value)
            {
                this->set(setting_name, value);
            });
    options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
            setting_name.data(), boost::program_options::value<std::string>()->composing()->notifier(on_program_option), accessor.getDescription(index)))); // NOLINT
}

void Settings::addToProgramOptionsAsMultitokens(boost::program_options::options_description & options) const
{
    addProgramOptionsAsMultitokens(*impl, options);
}

void Settings::addToClientOptions(Poco::Util::LayeredConfiguration &config, const boost::program_options::variables_map &options, bool repeated_settings) const
{
    for (const auto & setting : impl->all())
    {
        const auto & name = setting.getName();
        if (options.count(name))
        {
            if (repeated_settings)
                config.setString(name, options[name].as<Strings>().back());
            else
                config.setString(name, options[name].as<String>());
        }
    }
}

Field Settings::castValueUtil(std::string_view name, const Field & value)
{
    return SettingsImpl::castValueUtil(name, value);
}

String Settings::valueToStringUtil(std::string_view name, const Field & value)
{
    return SettingsImpl::valueToStringUtil(name, value);
}

Field Settings::stringToValueUtil(std::string_view name, const String & str)
{
    return SettingsImpl::stringToValueUtil(name, str);
}

bool Settings::hasBuiltin(std::string_view name)
{
    return SettingsImpl::hasBuiltin(name);
}

std::string_view Settings::resolveName(std::string_view name)
{
    return SettingsImpl::Traits::resolveName(name);
}

void Settings::checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path)
{
    SettingsImpl::checkNoSettingNamesAtTopLevel(config, config_path);
}

}
