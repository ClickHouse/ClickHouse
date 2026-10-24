#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `gcs` table function shares its argument grammar with `s3`, so a query can supply S3-only
# authentication (HMAC keys, a session token). With `use_native_gcs = 1` the native backend cannot
# use them; it must reject them explicitly instead of silently falling through to the server's
# Application Default Credentials. The rejection is thrown while the storage configuration is
# built, before any network client is constructed, so these queries are safe in every build type.
#
# The native backend only exists when the google-cloud-cpp SDK is compiled in; without it the
# check cannot be reached, so emit the expected output directly.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'")

if [ "$native_gcs_available" != "1" ]; then
    echo "hmac_pair: rejected"
    echo "hmac_with_session_token: rejected"
    exit 0
fi

result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs('https://storage.googleapis.com/test-bucket-04642/data.csv', 'AKIAEXAMPLEKEY04642', 'examplesecret') SETTINGS use_native_gcs = 1" 2>&1)
if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"HMAC key credentials are not supported by the native GCS backend"* ]]; then
    echo "hmac_pair: rejected"
else
    echo "hmac_pair: unexpected: $result"
fi

result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs('https://storage.googleapis.com/test-bucket-04642/data.csv', 'AKIAEXAMPLEKEY04642', 'examplesecret', 'examplesessiontoken', 'CSV') SETTINGS use_native_gcs = 1" 2>&1)
if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"HMAC key credentials are not supported by the native GCS backend"* ]]; then
    echo "hmac_with_session_token: rejected"
else
    echo "hmac_with_session_token: unexpected: $result"
fi
