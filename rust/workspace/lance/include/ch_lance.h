#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;
struct ArrowSchema;

typedef uint32_t ch_lance_error_kind;

enum
{
    CH_LANCE_ERROR_NONE = 0,
    CH_LANCE_ERROR_INVALID_ARGUMENT = 1,
    CH_LANCE_ERROR_NOT_FOUND = 2,
    CH_LANCE_ERROR_PERMISSION_DENIED = 3,
    CH_LANCE_ERROR_UNAUTHENTICATED = 4,
    CH_LANCE_ERROR_CORRUPT_DATA = 5,
    CH_LANCE_ERROR_UNSUPPORTED = 6,
    CH_LANCE_ERROR_VERSION_NOT_FOUND = 7,
    CH_LANCE_ERROR_STORAGE = 8,
    CH_LANCE_ERROR_INTERNAL = 9,
    /// Cooperative cancellation requested via ch_lance_cancel_scan (or equivalent).
    CH_LANCE_ERROR_CANCELLED = 10,
};

typedef uint32_t ch_lance_error_origin;

enum
{
    CH_LANCE_ERROR_ORIGIN_UNKNOWN = 0,
    CH_LANCE_ERROR_ORIGIN_LOCAL = 1,
    CH_LANCE_ERROR_ORIGIN_S3 = 2,
};

typedef struct ch_lance_error
{
    ch_lance_error_kind kind;
    ch_lance_error_origin origin;
    char * message;
} ch_lance_error;

typedef struct ch_lance_dataset ch_lance_dataset;
typedef struct ch_lance_scan ch_lance_scan;
/// Query-scoped cooperative cancel token. Thread-safe; may be shared across open/plan/count/scan.
typedef struct ch_lance_cancel_handle ch_lance_cancel_handle;

typedef struct ch_lance_dataset_options
{
    const char * uri;
    bool use_s3;
    const char * s3_region;
    const char * s3_endpoint;
    const char * s3_access_key_id;
    const char * s3_secret_access_key;
    const char * s3_session_token;
    const char * s3_role_arn;
    const char * s3_role_session_name;
    bool s3_use_environment_credentials;
    bool s3_no_sign_request;
    bool s3_allow_http;
    bool s3_virtual_hosted_style_request;
    /// Per-request HTTP timeout for S3 (object_store `timeout`). 0 = library default (30s).
    uint64_t s3_request_timeout_ms;
    /// TCP/connect timeout for S3 (object_store `connect_timeout`). 0 = library default (5s).
    uint64_t s3_connect_timeout_ms;
    /// Optional. When non-null, open can be interrupted by ch_lance_cancel_handle_cancel.
    ch_lance_cancel_handle * cancel;
} ch_lance_dataset_options;

typedef struct ch_lance_snapshot_info
{
    uint64_t version;
} ch_lance_snapshot_info;

typedef struct ch_lance_string_list
{
    const char * const * values;
    size_t size;
} ch_lance_string_list;

typedef struct ch_lance_scan_options
{
    uint64_t version;
    ch_lance_string_list projection;
    const char * predicate;
    bool need_only_count;
    uint64_t max_block_size;
    /// Soft upper bound on rows from the Lance scanner. 0 means unlimited.
    /// Corresponds to Scanner::limit(Some(limit), None).
    uint64_t limit;
    /// Optional. When non-null, planScan is interruptible and the resulting scan shares this token.
    ch_lance_cancel_handle * cancel;
    /// false (zero-init): ordered scan (SDK default, compatible).
    /// true: unordered scan; enables meaningful fragment_readahead.
    bool scan_unordered;
    /// 0 = leave Lance SDK default; >0 → Scanner::fragment_readahead.
    /// Only effective when scan_unordered is true.
    uint32_t fragment_readahead;
    /// 0 = leave Lance SDK default; >0 → Scanner::batch_readahead.
    uint32_t batch_readahead;
    /// 0 = leave Lance SDK default; >0 → Scanner::io_buffer_size.
    /// Do not set very small values (Lance may deadlock if a single batch exceeds the buffer).
    uint64_t io_buffer_size;
    /// Optional fragment id filter. null or size==0 → all fragments.
    /// Otherwise Scanner::with_fragments restricted to these ids (from the pinned version).
    const uint64_t * fragment_ids;
    size_t fragment_ids_size;
} ch_lance_scan_options;

typedef struct ch_lance_fragment_info
{
    uint64_t id;
    /// UINT64_MAX if unknown (Lance Option::None).
    uint64_t num_rows;
    /// 0 if unknown; best-effort sum of data file sizes.
    uint64_t size_bytes;
} ch_lance_fragment_info;

typedef struct ch_lance_runtime_config
{
    /// Number of Tokio worker threads. 0 means an automatic bounded default.
    uint32_t worker_threads;
} ch_lance_runtime_config;

typedef struct ch_lance_runtime_stats
{
    uint64_t open_dataset_calls;
    uint64_t plan_scan_calls;
    uint64_t next_batch_calls;
    uint64_t runtime_initialized;
} ch_lance_runtime_stats;

/// Ensure the process-wide Lance Tokio runtime exists. First successful call
/// wins for worker_threads; later calls are no-ops if the runtime is already up.
bool ch_lance_runtime_ensure(const ch_lance_runtime_config * config, ch_lance_error * error);
void ch_lance_get_runtime_stats(ch_lance_runtime_stats * out);

/// Query-scoped cancel handle. Create once per ReadSource / query unit of work.
ch_lance_cancel_handle * ch_lance_cancel_handle_create(void);
/// Thread-safe: signal cancellation. Concurrent with any in-flight open/plan/count/next_batch
/// that was given this handle (or a scan that inherited it).
void ch_lance_cancel_handle_cancel(ch_lance_cancel_handle * handle);
void ch_lance_cancel_handle_free(ch_lance_cancel_handle * handle);

ch_lance_dataset * ch_lance_open_dataset(const ch_lance_dataset_options * options, ch_lance_error * error);
void ch_lance_free_dataset(ch_lance_dataset * dataset);

bool ch_lance_current_snapshot(ch_lance_dataset * dataset, ch_lance_snapshot_info * snapshot, ch_lance_error * error);
bool ch_lance_export_schema(ch_lance_dataset * dataset, uint64_t version, struct ArrowSchema * schema, ch_lance_error * error);
/// `cancel` may be null (no cooperative cancel for this call).
bool ch_lance_total_rows(
    ch_lance_dataset * dataset,
    uint64_t version,
    uint64_t * rows,
    bool * has_value,
    ch_lance_cancel_handle * cancel,
    ch_lance_error * error);
bool ch_lance_count_rows(
    ch_lance_dataset * dataset,
    uint64_t version,
    const char * predicate,
    uint64_t * rows,
    bool * has_value,
    ch_lance_cancel_handle * cancel,
    ch_lance_error * error);
bool ch_lance_total_bytes(ch_lance_dataset * dataset, uint64_t * bytes, bool * has_value, ch_lance_error * error);

/// Lists fragments for an exact dataset version (checkout_exact_version).
/// On success with size>0: *out_list is allocated; free with ch_lance_free_fragment_list.
/// Empty datasets: *out_size=0 and *out_list=null.
/// cancel may be null.
bool ch_lance_list_fragments(
    ch_lance_dataset * dataset,
    uint64_t version,
    ch_lance_fragment_info ** out_list,
    size_t * out_size,
    ch_lance_cancel_handle * cancel,
    ch_lance_error * error);
void ch_lance_free_fragment_list(ch_lance_fragment_info * list, size_t size);

ch_lance_scan * ch_lance_plan_scan(ch_lance_dataset * dataset, const ch_lance_scan_options * options, ch_lance_error * error);
bool ch_lance_next_batch(ch_lance_scan * scan, struct ArrowArray * array, struct ArrowSchema * schema, bool * has_batch, ch_lance_error * error);
/// Thread-safe: request cooperative cancellation of a scan. Does not free the scan.
/// Concurrent with ch_lance_next_batch: wakes a pending next and causes it to return CANCELLED.
/// Safe to call multiple times. Does not race with ch_lance_free_scan if free happens only after
/// next_batch has returned (ClickHouse guarantees this via Scan lifetime).
void ch_lance_cancel_scan(ch_lance_scan * scan);
void ch_lance_free_scan(ch_lance_scan * scan);

void ch_lance_free_error(ch_lance_error * error);

#ifdef __cplusplus
}
#endif
