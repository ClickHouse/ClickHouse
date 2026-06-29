#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# Tag no-fasttest: needs s3
# Tag no-parallel: `SYSTEM RELOAD CONFIG` is global server state. Under the
# flaky check, the same test runs many times concurrently; the parallel
# `SYSTEM RELOAD CONFIG` calls serialize inside the server and a single
# instance has been observed waiting ~3 minutes before its reload starts,
# blowing the 180s per-test budget even though the actual reload work is
# fast. Serializing this test removes the queueing.
# Tag no-random-settings: SYSTEM RELOAD CONFIG combined with multipart S3 upload
# can exceed the flaky-check 180s timeout in debug builds when random settings
# (e.g. heavy filesystem cache injection, large reduce_blocking_parts_sleep_ms)
# inflate per-step latency. The bug under test is about config reload settings
# priority and is independent of these random settings.

# Verify that storage_configuration disk settings properly override global <s3>
# endpoint configuration for the DiskS3 path (S3ObjectStorage::applyNewSettings).
#
# The test depends on:
# - tests/config/config.d/storage_conf_04070.xml which defines s3_disk_04070
#   with s3_max_single_part_upload_size = 10000 (small, to force multipart upload).
# - tests/config/config.d/s3_settings_override.xml which configures a matching
#   global <s3> endpoint with max_single_part_upload_size = 100Mi.
#
# Without the fix, the global endpoint config would override the disk config
# setting, resulting in a single-part upload instead of multipart.
#
# SYSTEM RELOAD CONFIG triggers S3ObjectStorage::applyNewSettings, which merges
# the global <s3> endpoint settings with disk config settings. Without the fix
# the endpoint's 100 Mi value overrides the disk's 10 000 value.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_04070_s3_disk_override"

# Use a String column with random data so that LZ4 cannot compress it down
# below the 10 000-byte multipart threshold. A small row count keeps the test
# fast under the flaky check's random settings (debug build).
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_04070_s3_disk_override (s String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS storage_policy = 's3_04070'
"

# Force a config reload so that applyNewSettings merges global <s3> endpoint
# settings into the disk's S3ObjectStorage. On the buggy code path the
# endpoint's large max_single_part_upload_size wins; with the fix the disk
# config value (10 000) takes priority.
# In case of listen_try we can have 'Address already in use'
$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use'

# Disable `s3_check_objects_after_upload` because the size verification has
# been observed to be flaky against the local S3 mock under the flaky check
# (random settings combined with multipart upload), and this test only needs
# to verify that the multipart upload path is taken, not upload integrity.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_04070_s3_disk_override SELECT randomString(100) FROM numbers(500)
SETTINGS s3_check_objects_after_upload = 0
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# With s3_max_single_part_upload_size = 10000 from disk config, the data should
# be uploaded via multipart (CreateMultipartUpload + UploadPart + CompleteMultipartUpload).
# If the global endpoint config (100Mi) had incorrectly taken priority, it would be a single PutObject.
$CLICKHOUSE_CLIENT --query "
SELECT
    ProfileEvents['S3CreateMultipartUpload'] >= 1 AS has_multipart_create,
    ProfileEvents['S3UploadPart'] >= 1 AS has_upload_parts,
    ProfileEvents['S3CompleteMultipartUpload'] >= 1 AS has_multipart_complete
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%t_04070_s3_disk_override%'
    AND query LIKE '%INSERT%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY query_start_time DESC
LIMIT 1
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_04070_s3_disk_override"
