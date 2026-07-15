#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;
struct ArrowSchema;

typedef struct ch_lance_error
{
    char * message;
} ch_lance_error;

typedef struct ch_lance_dataset ch_lance_dataset;
typedef struct ch_lance_scan ch_lance_scan;

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
} ch_lance_dataset_options;

typedef struct ch_lance_snapshot_info
{
    uint64_t snapshot_id;
    uint64_t schema_id;
} ch_lance_snapshot_info;

typedef struct ch_lance_string_list
{
    const char * const * values;
    size_t size;
} ch_lance_string_list;

typedef struct ch_lance_scan_options
{
    uint64_t snapshot_id;
    ch_lance_string_list projection;
    const char * predicate;
    bool need_only_count;
    uint64_t max_block_size;
} ch_lance_scan_options;

ch_lance_dataset * ch_lance_open_dataset(const ch_lance_dataset_options * options, ch_lance_error * error);
void ch_lance_free_dataset(ch_lance_dataset * dataset);

bool ch_lance_current_snapshot(ch_lance_dataset * dataset, ch_lance_snapshot_info * snapshot, ch_lance_error * error);
bool ch_lance_export_schema(ch_lance_dataset * dataset, uint64_t snapshot_id, struct ArrowSchema * schema, ch_lance_error * error);
bool ch_lance_total_rows(ch_lance_dataset * dataset, uint64_t snapshot_id, uint64_t * rows, bool * has_value, ch_lance_error * error);
bool ch_lance_total_bytes(ch_lance_dataset * dataset, uint64_t * bytes, bool * has_value, ch_lance_error * error);

ch_lance_scan * ch_lance_plan_scan(ch_lance_dataset * dataset, const ch_lance_scan_options * options, ch_lance_error * error);
bool ch_lance_next_batch(ch_lance_scan * scan, struct ArrowArray * array, struct ArrowSchema * schema, bool * has_batch, ch_lance_error * error);
void ch_lance_free_scan(ch_lance_scan * scan);

void ch_lance_free_error(ch_lance_error * error);

#ifdef __cplusplus
}
#endif
