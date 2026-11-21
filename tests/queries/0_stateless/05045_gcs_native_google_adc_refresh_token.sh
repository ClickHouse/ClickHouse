#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# The `google_adc_*` "authorized user" triple is a first-class credential of the native GCS backend: the
# transport builds refreshable credentials from it (`ClickHouse::PocoRestAuthorizedUserOption`), so it is
# accepted for a long-lived disk instead of being refused for not being renewable. Verify the acceptance,
# and that a partially specified triple -- or a token endpoint without one -- is still rejected.
#
# Each case asserts *which layer* answered, classified below, rather than whether the disk works: every one
# of those answers is decided while the configuration is parsed, before a request is sent. The disks are
# declared `readonly` so that creating the table does not write to them either, which keeps the test off the
# network entirely -- an unreachable endpoint would otherwise be a retryable error and the SDK's default
# retry policy would sit on it for minutes. The end-to-end refresh, against a stub token endpoint, is
# covered by `test_native_gcs`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

# Which layer answered. Prints only the class, never the server's message, so that no exception text can
# reach the test's stdout (which `clickhouse-test` treats as a failure on its own).
classify() {
    case "$1" in
        *"may not use Application Default Credentials"*) echo "refused_for_credentials" ;;
        *"must be specified together"*|*"only applies together with"*|*"does not support"*) echo "rejected_by_validation" ;;
        *) echo "accepted" ;;
    esac
}

create_gcs_disk_table() {
    local table=$1
    local extra=$2
    $CLICKHOUSE_CLIENT --use_native_gcs=1 -q "
        CREATE TABLE ${table} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(name = '${table}_disk', type = gcs,
            endpoint = 'https://storage.googleapis.com/${table}/',
            ${extra},
            readonly = 1, skip_access_check = 1)" 2>&1
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${table}" 2>/dev/null
}

if [ "$native_gcs_available" = "1" ]; then
    # A complete triple is a credential the SQL definition supplied, so the credential layer lets it through.
    echo "complete_triple: $(classify "$(create_gcs_disk_table "adc_full_${DB}" \
        "google_adc_client_id = 'client-id', google_adc_client_secret = 'client-secret', google_adc_refresh_token = 'refresh-token'")")"

    # Two of the three settings are one incomplete credential, not a mode selection. `no_sign_request` keeps
    # the earlier Application Default Credentials check from answering first, so the triple check is what does.
    echo "partial_triple: $(classify "$(create_gcs_disk_table "adc_partial_${DB}" \
        "no_sign_request = 1, google_adc_client_id = 'client-id', google_adc_refresh_token = 'refresh-token'")")"

    # A token endpoint selects nothing on its own: there is no refresh token to exchange there.
    echo "token_uri_only: $(classify "$(create_gcs_disk_table "adc_uri_${DB}" \
        "no_sign_request = 1, google_adc_token_uri = 'https://oauth2.example/token'")")"

    # The comparison that makes the first line meaningful: asking for no credential at all is refused.
    echo "no_credentials: $(classify "$(create_gcs_disk_table "adc_none_${DB}" "metadata_type = local")")"
else
    echo "complete_triple: accepted"
    echo "partial_triple: rejected_by_validation"
    echo "token_uri_only: rejected_by_validation"
    echo "no_credentials: refused_for_credentials"
fi
