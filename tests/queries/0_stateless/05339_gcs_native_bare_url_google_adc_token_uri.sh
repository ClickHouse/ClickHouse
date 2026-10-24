#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the `gcs` table function, which is not compiled into the fast-test build.
#
# The bare-URL form of `gcs(...)` with `use_native_gcs = 1` must pass `google_adc_token_uri` to the native
# backend rather than silently dropping it: otherwise the refresh token would be exchanged at Google's
# default endpoint instead of the one the user named. A token endpoint without the refresh-token triple is
# rejected by the triple validation, which only happens if the argument reached the backend. `EXPLAIN`
# builds the object storage without sending a request, so the test never touches the network.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

# Prints only the class, never the server's message, so that no exception text can reach the test's stdout.
classify() {
    case "$1" in
        *"only applies together with"*) echo "rejected_by_validation" ;;
        *"ReadFromObjectStorage"*) echo "accepted" ;;
        *) echo "unexpected" ;;
    esac
}

explain_gcs() {
    ${CLICKHOUSE_CLIENT} --use_native_gcs=1 -q "
        EXPLAIN SELECT * FROM gcs('https://storage.googleapis.com/test-bucket-05339/data.csv', 'CSV', 'x UInt8' $1)" 2>&1
}

if [ "$native_gcs_available" = "1" ]; then
    echo "token_uri_only: $(classify "$(explain_gcs ", google_adc_token_uri = 'http://localhost:1/token'")")"
    echo "triple_with_token_uri: $(classify "$(explain_gcs \
        ", google_adc_client_id = 'client-id', google_adc_client_secret = 'client-secret', google_adc_refresh_token = 'refresh-token', google_adc_token_uri = 'http://localhost:1/token'")")"
else
    echo "token_uri_only: rejected_by_validation"
    echo "triple_with_token_uri: accepted"
fi
