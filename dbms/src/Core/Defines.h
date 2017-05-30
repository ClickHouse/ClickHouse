#pragma once

#define DBMS_NAME "ClickHouse"
#define DBMS_VERSION_MAJOR 1
#define DBMS_VERSION_MINOR 1

#define DBMS_DEFAULT_HOST "localhost"
#define DBMS_DEFAULT_PORT 9000
#define DBMS_DEFAULT_PORT_STR #DBMS_DEFAULT_PORT
#define DBMS_DEFAULT_HTTP_PORT 8123
#define DBMS_DEFAULT_CONNECT_TIMEOUT_SEC 10
#define DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS 50
#define DBMS_DEFAULT_SEND_TIMEOUT_SEC 300
#define DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC 300
/// Timeout for synchronous request-result protocol call (like Ping or TablesStatus).
#define DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC 5
#define DBMS_DEFAULT_POLL_INTERVAL 10

/// The size of the I/O buffer by default.
#define DBMS_DEFAULT_BUFFER_SIZE 1048576ULL

/// When writing data, a buffer of `max_compress_block_size` size is allocated for compression. When the buffer overflows or if into the buffer
/// more or equal data is written than `min_compress_block_size`, then with the next mark, the data will also compressed
/// As a result, for small columns (numbers 1-8 bytes), with index_granularity = 8192, the block size will be 64 KB.
/// And for large columns (Title - string ~100 bytes), the block size will be ~819 KB. Due to this, the compression ratio almost does not get worse.
#define DEFAULT_MIN_COMPRESS_BLOCK_SIZE 65536
#define DEFAULT_MAX_COMPRESS_BLOCK_SIZE 1048576

/** Which blocks by default read the data (by number of rows).
  * Smaller values give better cache locality, less consumption of RAM, but more overhead to process the query.
  */
#define DEFAULT_BLOCK_SIZE 65536

/** Which blocks should be formed for insertion into the table, if we control the formation of blocks.
  * (Sometimes the blocks are inserted exactly such blocks that have been read / transmitted from the outside, and this parameter does not affect their size.)
  * More than DEFAULT_BLOCK_SIZE, because in some tables a block of data on the disk is created for each block (quite a big thing),
  *  and if the parts were small, then it would be costly then to combine them.
  */
#define DEFAULT_INSERT_BLOCK_SIZE 1048576

/** The same, but for merge operations. Less DEFAULT_BLOCK_SIZE for saving RAM (since all the columns are read).
  * Significantly less, since there are 10-way mergers.
  */
#define DEFAULT_MERGE_BLOCK_SIZE 8192

#define DEFAULT_MAX_QUERY_SIZE 262144
#define SHOW_CHARS_ON_SYNTAX_ERROR 160L
#define DEFAULT_MAX_DISTRIBUTED_CONNECTIONS 1024
#define DEFAULT_INTERACTIVE_DELAY 100000
#define DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE 1024
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3
/// each period reduces the error counter by 2 times
/// too short a period can cause errors to disappear immediately after creation.
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD (2 * DBMS_DEFAULT_SEND_TIMEOUT_SEC)
#define DEFAULT_QUERIES_QUEUE_WAIT_TIME_MS 5000 /// Maximum waiting time in the request queue.
#define DBMS_DEFAULT_BACKGROUND_POOL_SIZE 16

/// Used in the `reserve` method, when the number of rows is known, but their dimensions are unknown.
#define DBMS_APPROX_STRING_SIZE 64

/// Name suffix for the column containing the array offsets.
#define ARRAY_SIZES_COLUMN_NAME_SUFFIX ".size"

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES 50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS 51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO 51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO 54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE 54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060
#define DBMS_MIN_REVISION_WITH_TABLES_STATUS 54226

/// Version of ClickHouse TCP protocol. Set to git tag with latest protocol change.
#define DBMS_TCP_PROTOCOL_VERSION 54226

#define DBMS_DISTRIBUTED_DIRECTORY_MONITOR_SLEEP_TIME_MS 100

/// The boundary on which the blocks for asynchronous file operations should be aligned.
#define DEFAULT_AIO_FILE_BLOCK_SIZE 4096

#define DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS 7500

#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))

#define PLATFORM_NOT_SUPPORTED "The only supported platforms are x86_64 and AArch64 (work in progress)"

#if !defined(__x86_64__) && !defined(__aarch64__)
    #error PLATFORM_NOT_SUPPORTED
#endif

/// Check for presence of address sanitizer
#if defined(__has_feature)
    #if __has_feature(address_sanitizer)
        #define ADDRESS_SANITIZER 1
    #endif
#elif defined(__SANITIZE_ADDRESS__)
    #define ADDRESS_SANITIZER 1
#endif

#if defined(__has_feature)
    #if __has_feature(thread_sanitizer)
        #define THREAD_SANITIZER 1
    #endif
#elif defined(__SANITIZE_THREAD__)
    #define THREAD_SANITIZER 1
#endif
