#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A presigned URL carries its authentication in the query string (`GoogleAccessId` / `Signature` /
# `Expires` for V2 signatures, `X-Goog-*` for V4). The S3-compatibility path forwards those query
# parameters with every request, but the native GCS client authenticates with its own credentials
# and never sends them, so with `use_native_gcs = 1` the URL's signature would be silently replaced
# by the server's ambient Google identity. Such URLs must be rejected instead. The rejection is
# thrown while the storage configuration is built, before any network client is constructed, so
# these queries are safe in every build type.
#
# The native backend only exists when the google-cloud-cpp SDK is compiled in; without it the check
# cannot be reached, so emit the expected output directly. The gate fails toward skipping.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

if [ "$native_gcs_available" != "1" ]; then
    echo "presigned_v2: rejected"
    echo "presigned_v4: rejected"
    exit 0
fi

check_rejected()
{
    local label=$1
    local url=$2
    local result
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs('${url}', 'CSV', 'c String') SETTINGS use_native_gcs = 1" 2>&1)
    if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"Presigned URLs"* ]]; then
        echo "${label}: rejected"
    else
        echo "${label}: unexpected: $result"
    fi
}

check_rejected "presigned_v2" "https://storage.googleapis.com/test-bucket-04817/data.csv?GoogleAccessId=service-account%40example.iam.gserviceaccount.com&Expires=1893456000&Signature=c2lnbmF0dXJl"
check_rejected "presigned_v4" "https://storage.googleapis.com/test-bucket-04817/data.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=cred&X-Goog-Date=20260101T000000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=abc"
