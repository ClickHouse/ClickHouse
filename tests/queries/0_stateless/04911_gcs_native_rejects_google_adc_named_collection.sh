#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native GCS client materializes `google_adc_*` refresh-token credentials into one access token.
# A long-running query can outlive that token, and the client cannot refresh it. Native SQL must
# therefore reject this named-collection credential mode before it opens a network connection.
#
# The native backend exists only with google-cloud-cpp. Without it, produce the expected result.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

if [ "$native_gcs_available" != "1" ]; then
    echo "google_adc_named_collection: rejected"
    exit 0
fi

collection="${CLICKHOUSE_DATABASE}_gcs_google_adc"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${collection}"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${collection} AS
    url = 'https://storage.googleapis.com/test-bucket-04911/data.csv',
    google_adc_client_id = 'client-id',
    google_adc_client_secret = 'client-secret',
    google_adc_refresh_token = 'refresh-token',
    format = 'CSV'"

result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs(${collection}) SETTINGS use_native_gcs = 1" 2>&1)
if [[ "$result" == *"BAD_ARGUMENTS"* && "$result" == *"does not support \`google_adc_*\` refresh-token credentials"* ]]; then
    echo "google_adc_named_collection: rejected"
else
    echo "google_adc_named_collection: unexpected: $result"
fi

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${collection}"
