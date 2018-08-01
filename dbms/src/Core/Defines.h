#pragma once

#define DBMS_DEFAULT_HOST "localhost"
#define DBMS_DEFAULT_PORT 9000
#define DBMS_DEFAULT_SECURE_PORT 9440
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

#define SHOW_CHARS_ON_SYNTAX_ERROR ptrdiff_t(160)
#define DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE 1024
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3
/// each period reduces the error counter by 2 times
/// too short a period can cause errors to disappear immediately after creation.
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD (2 * DBMS_DEFAULT_SEND_TIMEOUT_SEC)

#define DBMS_MIN_REVISION_WITH_CLIENT_INFO 54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE 54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060
#define DBMS_MIN_REVISION_WITH_TABLES_STATUS 54226
#define DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE 54337
#define DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME 54372
#define DBMS_MIN_REVISION_WITH_VERSION_PATCH 54401

/// Version of ClickHouse TCP protocol. Set to git tag with latest protocol change.
#define DBMS_TCP_PROTOCOL_VERSION 54226

/// The boundary on which the blocks for asynchronous file operations should be aligned.
#define DEFAULT_AIO_FILE_BLOCK_SIZE 4096

#define DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS 7500

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1

// more aliases: https://mailman.videolan.org/pipermail/x264-devel/2014-May/010660.html

#if defined(_MSC_VER)
    #define ALWAYS_INLINE __forceinline
    #define NO_INLINE static __declspec(noinline)
    #define MAY_ALIAS
#else
    #define ALWAYS_INLINE __attribute__((__always_inline__))
    #define NO_INLINE __attribute__((__noinline__))
    #define MAY_ALIAS __attribute__((__may_alias__))
#endif


#define PLATFORM_NOT_SUPPORTED "The only supported platforms are x86_64 and AArch64 (work in progress)"

#if !defined(__x86_64__) && !defined(__aarch64__)
//    #error PLATFORM_NOT_SUPPORTED
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
