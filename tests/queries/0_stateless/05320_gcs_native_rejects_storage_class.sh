#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `gcs` table function and a dynamic `gcs` disk share their argument grammar with `s3`, so they
# accept an S3 storage class. The native backend uploads objects in the bucket's default storage
# class, so it must reject the option instead of silently ignoring the requested tier. The
# rejection is thrown while the configuration is built, before any request is sent.
#
# The native backend only exists when the google-cloud-cpp SDK is compiled in; without it the
# check cannot be reached, so emit the expected output directly.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'")

if [ "$native_gcs_available" != "1" ]; then
    echo "table_function: rejected"
    echo "disk: rejected"
    exit 0
fi

result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs('https://storage.googleapis.com/test-bucket-05320/data.csv', NOSIGN, 'CSV', 'x UInt8', storage_class_name = 'STANDARD_IA') SETTINGS use_native_gcs = 1" 2>&1)
if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"storage_class_name"* && "$result" == *"is not supported by the native GCS backend"* ]]; then
    echo "table_function: rejected"
else
    echo "table_function: unexpected: $result"
fi

result=$(${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE gcs_storage_class_${CLICKHOUSE_DATABASE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = 'gcs_storage_class_disk_${CLICKHOUSE_DATABASE}', type = object_storage, object_storage_type = gcs,
        metadata_type = local,
        endpoint = 'https://storage.googleapis.com/test-bucket-05320/${CLICKHOUSE_DATABASE}/',
        no_sign_request = true,
        s3_storage_class = 'STANDARD_IA')" 2>&1)
if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"does not support \`s3_storage_class\`"* ]]; then
    echo "disk: rejected"
else
    echo "disk: unexpected: $result"
fi
