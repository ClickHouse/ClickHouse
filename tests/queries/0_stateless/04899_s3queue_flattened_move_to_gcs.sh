#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A move that flattens paths needs a write that refuses to overwrite, which GCS does not support.
# CREATE and ALTER refuse such a move to GCS; nothing here talks to GCS.

KEEPER_PATH="/clickhouse/test_${CLICKHOUSE_DATABASE}/s3queue_gcs_move"
GCS_SOURCE="https://storage.googleapis.com/bucket-04899/data/"
OTHER_SOURCE="http://whatever-we-dont-care:9001/root/data-04899/"
TO_GCS="after_processing_move_uri = 'https://storage.googleapis.com/dst-04899/', after_processing_move_access_key_id = 'k', after_processing_move_secret_access_key = 's'"

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o -m1 'BAD_ARGUMENTS' || echo 'OK'
}

function create()
{
    run "CREATE TABLE $1 (a UInt32) ENGINE = S3Queue('$2', 'username', 'password', CSV)
        SETTINGS mode = 'unordered', keeper_path = '$KEEPER_PATH/$1', after_processing = 'move', $3"
}

echo 'within a GCS bucket'
create t_gcs_flat "$GCS_SOURCE" "after_processing_move_prefix = 'done/'"
create t_gcs_keep "$GCS_SOURCE" "after_processing_move_prefix = 'done/', after_processing_move_preserve_path = 1"
echo 'to a GCS bucket'
create t_to_gcs_flat "$OTHER_SOURCE" "$TO_GCS, after_processing_move_prefix = 'done/'"
create t_to_gcs_no_prefix "$OTHER_SOURCE" "$TO_GCS"
echo 'not GCS'
create t_s3_flat "$OTHER_SOURCE" "after_processing_move_prefix = 'done/'"
echo 'ALTER'
run "ALTER TABLE t_gcs_keep MODIFY SETTING after_processing_move_preserve_path = 0"
run "ALTER TABLE t_s3_flat MODIFY SETTING $TO_GCS"
run "ALTER TABLE t_to_gcs_no_prefix MODIFY SETTING after_processing_move_prefix = 'done/'"
run "ALTER TABLE t_gcs_keep MODIFY SETTING polling_max_timeout_ms = 1000"

for table in t_gcs_flat t_gcs_keep t_to_gcs_flat t_to_gcs_no_prefix t_s3_flat; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
done
