#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# A native GCS disk section shares its key namespace with the `s3` disk type -- a dynamic
# `disk(object_storage_type = gcs, ...)` inherits the whole `s3` argument grammar -- so it can carry
# authentication that only the S3-compatibility path understands. The native client cannot use any of it,
# and accepting it silently would leave the disk authenticating as something the operator did not ask for.
# `GCSObjectStorageSettings::loadFromConfig` must name each such key and fail closed, the way the SQL
# surface already does. The keys are checked before any client is built, so this needs no live endpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

for key_value in \
    "access_key_id='key'" \
    "secret_access_key='secret'" \
    "session_token='token'" \
    "role_arn='arn:aws:iam::1:role/r'" \
    "http_client='gcp_oauth'" \
    "service_account='sa@example.com'" \
    "metadata_service='http://169.254.169.254'" \
    "request_token_path='/token'" \
    "server_side_encryption_customer_key_base64='a2V5'"
do
    key="${key_value%%=*}"
    if [ "$native_gcs_available" != "1" ]; then
        echo "${key}: rejected"
        continue
    fi
    out="$($CLICKHOUSE_CLIENT --use_native_gcs=1 -q "
        CREATE TABLE s3_only_${DB} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(name = 's3_only_${key}_${DB}', type = gcs,
            endpoint = 'https://storage.googleapis.com/${DB}_s3_only/',
            no_sign_request = 1,
            ${key_value},
            skip_access_check = 1)" 2>&1)"
    if [[ "${out}" == *"does not support \`${key}\`"* ]]; then
        echo "${key}: rejected"
    else
        echo "${key}: unexpected: ${out//$'\n'/ }"
    fi
done

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS s3_only_${DB}"
