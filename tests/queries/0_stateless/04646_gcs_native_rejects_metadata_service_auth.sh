#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The S3-compatibility path can authenticate through the GCP metadata service (`http_client = gcp_oauth`
# with `service_account`, `metadata_service`, `request_token_path`), requesting a token for the *named*
# service account. The native backend has no equivalent: Application Default Credentials only ever use
# the VM's default service account, so with `use_native_gcs = 1` these settings must be rejected instead
# of silently changing the requested identity. The rejection is thrown while the storage configuration is
# built, before any network client is constructed, so these queries are safe in every build type.
#
# The native backend only exists when the google-cloud-cpp SDK is compiled in; without it the check
# cannot be reached, so emit the expected output directly. The gate fails toward skipping.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

if [ "$native_gcs_available" != "1" ]; then
    echo "metadata_service_auth: rejected"
    exit 0
fi

collection="${CLICKHOUSE_DATABASE}_gcs_gcp_oauth"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${collection}"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${collection} AS
    url = 'https://storage.googleapis.com/test-bucket-04646/data.csv',
    http_client = 'gcp_oauth',
    service_account = 'my-robot',
    format = 'CSV'"

result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs(${collection}) SETTINGS use_native_gcs = 1" 2>&1)
if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"Metadata-service OAuth settings"* ]]; then
    echo "metadata_service_auth: rejected"
else
    echo "metadata_service_auth: unexpected: $result"
fi

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${collection}"
