#pragma once

#include <base/defines.h>

#define DBMS_DEFAULT_PORT 9000
#define DBMS_DEFAULT_SECURE_PORT 9440
#define DBMS_DEFAULT_CONNECT_TIMEOUT_SEC 10
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
#define DEFAULT_BLOCK_SIZE 65505    /// 65536 minus 16 + 15 bytes padding that we usually have in arrays

/** Which blocks should be formed for insertion into the table, if we control the formation of blocks.
  * (Sometimes the blocks are inserted exactly such blocks that have been read / transmitted from the outside, and this parameter does not affect their size.)
  * More than DEFAULT_BLOCK_SIZE, because in some tables a block of data on the disk is created for each block (quite a big thing),
  *  and if the parts were small, then it would be costly then to combine them.
  */
#define DEFAULT_INSERT_BLOCK_SIZE 1048545   /// 1048576 minus 16 + 15 bytes padding that we usually have in arrays

/** The same, but for merge operations. Less DEFAULT_BLOCK_SIZE for saving RAM (since all the columns are read).
  * Significantly less, since there are 10-way mergers.
  */
#define DEFAULT_MERGE_BLOCK_SIZE 8192

#define DEFAULT_TEMPORARY_LIVE_VIEW_TIMEOUT_SEC 5
#define DEFAULT_PERIODIC_LIVE_VIEW_REFRESH_SEC 60
#define SHOW_CHARS_ON_SYNTAX_ERROR ptrdiff_t(160)
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3
/// each period reduces the error counter by 2 times
/// too short a period can cause errors to disappear immediately after creation.
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD 60
/// replica error max cap, this is to prevent replica from accumulating too many errors and taking to long to recover.
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT 1000

/// The boundary on which the blocks for asynchronous file operations should be aligned.
#define DEFAULT_AIO_FILE_BLOCK_SIZE 4096

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1
/// Maximum number of http-connections between two endpoints
/// the number is unmotivated
#define DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT 15

#define DBMS_DEFAULT_PATH "/var/lib/clickhouse/"

/// Actually, there may be multiple acquisitions of different locks for a given table within one query.
/// Check with IStorage class for the list of possible locks
#define DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC 120

/// Default limit on recursion depth of recursive descend parser.
#define DBMS_DEFAULT_MAX_PARSER_DEPTH 1000

/// Default limit on query size.
#define DBMS_DEFAULT_MAX_QUERY_SIZE 262144

/// Max depth of hierarchical dictionary
#define DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH 1000

/// Query profiler cannot work with sanitizers.
/// Sanitizers are using quick "frame walking" stack unwinding (this implies -fno-omit-frame-pointer)
/// And they do unwinding frequently (on every malloc/free, thread/mutex operations, etc).
/// They change %rbp during unwinding and it confuses libunwind if signal comes during sanitizer unwinding
///  and query profiler decide to unwind stack with libunwind at this moment.
///
/// Symptoms: you'll get silent Segmentation Fault - without sanitizer message and without usual ClickHouse diagnostics.
///
/// Look at compiler-rt/lib/sanitizer_common/sanitizer_stacktrace.h
#if !defined(SANITIZER)
#define QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS 1000000000
#else
#define QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS 0
#endif
