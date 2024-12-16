#pragma once

#include <base/defines.h>
#include <base/unit.h>

namespace DB
{

static constexpr auto DBMS_DEFAULT_PORT = 9000;
static constexpr auto DBMS_DEFAULT_SECURE_PORT = 9440;

static constexpr auto DBMS_DEFAULT_CONNECT_TIMEOUT_SEC = 10;
static constexpr auto DBMS_DEFAULT_SEND_TIMEOUT_SEC = 300;
static constexpr auto DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC = 300;

/// Timeout for synchronous request-result protocol call (like Ping or TablesStatus).
static constexpr auto DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC = 5;
static constexpr auto DBMS_DEFAULT_POLL_INTERVAL = 10;

/// The size of the I/O buffer by default.
static constexpr auto DBMS_DEFAULT_BUFFER_SIZE = 1048576ULL;

/// The initial size of adaptive I/O buffer by default.
static constexpr auto DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE = 16384ULL;

static constexpr auto PADDING_FOR_SIMD = 64;

/** Which blocks by default read the data (by number of rows).
  * Smaller values give better cache locality, less consumption of RAM, but more overhead to process the query.
  */
static constexpr auto DEFAULT_BLOCK_SIZE
    = 65409; /// 65536 - PADDING_FOR_SIMD - (PADDING_FOR_SIMD - 1) bytes padding that we usually have in = arrays

/** Which blocks should be formed for insertion into the table, if we control the formation of blocks.
  * (Sometimes the blocks are inserted exactly such blocks that have been read / transmitted from the outside, and this parameter does not affect their size.)
  * More than DEFAULT_BLOCK_SIZE, because in some tables a block of data on the disk is created for each block (quite a big thing),
  *  and if the parts were small, then it would be costly then to combine them.
  */
static constexpr auto DEFAULT_INSERT_BLOCK_SIZE
    = 1048449; /// 1048576 - PADDING_FOR_SIMD - (PADDING_FOR_SIMD - 1) bytes padding that we usually have in arrays

static constexpr auto SHOW_CHARS_ON_SYNTAX_ERROR = ptrdiff_t(160);
/// each period reduces the error counter by 2 times
/// too short a period can cause errors to disappear immediately after creation.
static constexpr auto DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD = 60;
/// replica error max cap, this is to prevent replica from accumulating too many errors and taking too long to recover.
static constexpr auto DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT = 1000;

/// The boundary on which the blocks for asynchronous file operations should be aligned.
static constexpr auto DEFAULT_AIO_FILE_BLOCK_SIZE = 4096;

static constexpr auto DEFAULT_HTTP_READ_BUFFER_TIMEOUT = 30;
static constexpr auto DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT = 1;
/// Maximum number of http-connections between two endpoints
/// the number is unmotivated
static constexpr auto DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT = 15;

static constexpr auto DEFAULT_TCP_KEEP_ALIVE_TIMEOUT = 290;
static constexpr auto DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT = 30;
static constexpr auto DEFAULT_HTTP_KEEP_ALIVE_MAX_REQUEST = 1000;

static constexpr auto DBMS_DEFAULT_PATH = "/var/lib/clickhouse/";

/// Actually, there may be multiple acquisitions of different locks for a given table within one query.
/// Check with IStorage class for the list of possible locks
static constexpr auto DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC = 120;

/// Default limit on recursion depth of recursive descend parser.
static constexpr auto DBMS_DEFAULT_MAX_PARSER_DEPTH = 1000;
/// Default limit on the amount of backtracking of recursive descend parser.
static constexpr auto DBMS_DEFAULT_MAX_PARSER_BACKTRACKS = 1000000;

/// Default limit on recursive CTE evaluation depth.
static constexpr auto DBMS_RECURSIVE_CTE_MAX_EVALUATION_DEPTH = 1000;

/// Default limit on query size.
static constexpr auto DBMS_DEFAULT_MAX_QUERY_SIZE = 262144;

/// Max depth of hierarchical dictionary
static constexpr auto DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH = 1000;

#ifdef OS_LINUX
#define DBMS_DEFAULT_PAGE_CACHE_USE_MADV_FREE true
#else
/// On Mac OS, MADV_FREE is not lazy, so page_cache_use_madv_free should be disabled.
/// On FreeBSD, it may work but we haven't tested it.
#define DBMS_DEFAULT_PAGE_CACHE_USE_MADV_FREE false
#endif


/// Default maximum (total and entry) sizes and policies of various caches
static constexpr auto DEFAULT_UNCOMPRESSED_CACHE_POLICY = "SLRU";
static constexpr auto DEFAULT_UNCOMPRESSED_CACHE_MAX_SIZE = 0_MiB;
static constexpr auto DEFAULT_UNCOMPRESSED_CACHE_SIZE_RATIO = 0.5l;
static constexpr auto DEFAULT_MARK_CACHE_POLICY = "SLRU";
static constexpr auto DEFAULT_MARK_CACHE_MAX_SIZE = 5_GiB;
static constexpr auto DEFAULT_MARK_CACHE_SIZE_RATIO = 0.5l;
static constexpr auto DEFAULT_INDEX_UNCOMPRESSED_CACHE_POLICY = "SLRU";
static constexpr auto DEFAULT_INDEX_UNCOMPRESSED_CACHE_MAX_SIZE = 0;
static constexpr auto DEFAULT_INDEX_UNCOMPRESSED_CACHE_SIZE_RATIO = 0.5;
static constexpr auto DEFAULT_INDEX_MARK_CACHE_POLICY = "SLRU";
static constexpr auto DEFAULT_INDEX_MARK_CACHE_MAX_SIZE = 5_GiB;
static constexpr auto DEFAULT_INDEX_MARK_CACHE_SIZE_RATIO = 0.3;
static constexpr auto DEFAULT_MMAP_CACHE_MAX_SIZE = 1_KiB; /// chosen by rolling dice
static constexpr auto DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_SIZE = 128_MiB;
static constexpr auto DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_ENTRIES = 10'000;
static constexpr auto DEFAULT_QUERY_CACHE_MAX_SIZE = 1_GiB;
static constexpr auto DEFAULT_QUERY_CACHE_MAX_ENTRIES = 1024uz;
static constexpr auto DEFAULT_QUERY_CACHE_MAX_ENTRY_SIZE_IN_BYTES = 1_MiB;
static constexpr auto DEFAULT_QUERY_CACHE_MAX_ENTRY_SIZE_IN_ROWS = 30'000'000uz;

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
static constexpr auto QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS = 1000000000;
#else
static constexpr auto QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS = 0;
#endif

}
