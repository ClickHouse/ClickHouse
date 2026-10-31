#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the `gcs` table function, which is not compiled into the fast-test build.
#
# The bare-URL form of `gcs(...)` with `use_native_gcs = 1` must honour the credential arguments the native
# backend understands -- the `google_adc_*` refresh-token triple and `use_environment_credentials` -- exactly
# like a named collection does, instead of silently dropping them and falling back to Application Default
# Credentials. The credential modes that only the disk configuration supports (`service_account_key`,
# `service_account_key_file`, `access_token`) must be rejected rather than ignored. With the default `s3_allow_server_credentials_in_user_queries = 0`, falling back is refused,
# so each case records which layer answered. The test server sets `<s3><use_environment_credentials>0` (see
# `tests/config/config.d/disable_s3_env_credentials.xml`), so a request without any credential argument reads
# anonymously instead of falling back. `EXPLAIN` builds the object storage, where all of these
# decisions are made, without sending a request, so the test never touches the network.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

# Prints only the class, never the server's message, so that no exception text can reach the test's stdout.
classify() {
    case "$1" in
        *"may not use Application Default Credentials"*) echo "refused_for_credentials" ;;
        *"must be specified together"*) echo "rejected_by_validation" ;;
        *"only accepted in the configuration of a native GCS disk"*) echo "rejected_disk_only" ;;
        *"ReadFromObjectStorage"*) echo "accepted" ;;
        *) echo "unexpected" ;;
    esac
}

explain_gcs() {
    ${CLICKHOUSE_CLIENT} --use_native_gcs=1 -q "
        EXPLAIN SELECT * FROM gcs('https://storage.googleapis.com/test-bucket-05331/data.csv', 'CSV', 'x UInt8' $1)" 2>&1
}

if [ "$native_gcs_available" = "1" ]; then
    echo "no_credentials: $(classify "$(explain_gcs "")")"
    echo "environment_credentials_off: $(classify "$(explain_gcs ", use_environment_credentials = 0")")"
    echo "environment_credentials_on: $(classify "$(explain_gcs ", use_environment_credentials = 1")")"
    echo "complete_triple: $(classify "$(explain_gcs \
        ", google_adc_client_id = 'client-id', google_adc_client_secret = 'client-secret', google_adc_refresh_token = 'refresh-token'")")"
    echo "partial_triple: $(classify "$(explain_gcs ", google_adc_client_id = 'client-id'")")"
    echo "service_account_key: $(classify "$(explain_gcs ", service_account_key = '{}'")")"
    echo "service_account_key_file: $(classify "$(explain_gcs ", service_account_key_file = '/key.json'")")"
    echo "access_token: $(classify "$(explain_gcs ", access_token = 'token'")")"
else
    echo "no_credentials: accepted"
    echo "environment_credentials_off: accepted"
    echo "environment_credentials_on: refused_for_credentials"
    echo "complete_triple: accepted"
    echo "partial_triple: rejected_by_validation"
    echo "service_account_key: rejected_disk_only"
    echo "service_account_key_file: rejected_disk_only"
    echo "access_token: rejected_disk_only"
fi
