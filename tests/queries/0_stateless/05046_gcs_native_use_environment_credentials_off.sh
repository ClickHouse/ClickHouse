#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# `use_environment_credentials = 0` means "do not resolve an ambient, server-managed identity". On the
# S3-compatibility path such a request goes unsigned; on the native GCS path the ambient identity is
# Application Default Credentials, so the same flag has to suppress those. Dropping it would silently
# turn an explicit credential opt-out into "authenticate as the server".
#
# With the default `s3_allow_server_credentials_in_user_queries = 0`, a native GCS configuration that
# asks for nothing is refused for resolving Application Default Credentials, while one that opted out of
# them is let through -- so each case below records *which layer answered*, and the pair is a comparison
# rather than an assertion about an absent message. Every one of those answers is decided while the
# configuration is parsed, before a request is sent, and the disks are declared `readonly` so that creating
# the table does not write to them either: the test never touches the network. The complementary positive --
# a URL-only named collection actually reading a bucket anonymously, the shape that carries this flag by
# default -- is covered by `test_native_gcs` against the emulator.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

# Prints only the class, never the server's message, so that no exception text can reach the test's stdout
# (which `clickhouse-test` treats as a failure on its own).
classify() {
    case "$1" in
        *"may not use Application Default Credentials"*) echo "refused_for_credentials" ;;
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
    # A dynamic native GCS disk carries the key through the shared argument grammar, so it is one of the
    # credential forms that satisfies the restriction, like `no_sign_request`.
    echo "disk_environment_credentials_off: $(classify "$(create_gcs_disk_table "env_off_${DB}" \
        "use_environment_credentials = 0")")"
    echo "disk_no_credentials: $(classify "$(create_gcs_disk_table "env_default_${DB}" \
        "metadata_type = local")")"

    # The same comparison on the table-function surface. A collection that opts *into* environment
    # credentials is refused while the configuration is projected, before a client is built.
    collection="${DB}_gcs_env_on"
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${collection}"
    ${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${collection} AS
        url = 'https://storage.googleapis.com/test-bucket-05046/data.csv',
        use_environment_credentials = 1,
        format = 'CSV'"
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM gcs(${collection}) SETTINGS use_native_gcs = 1" 2>&1)
    echo "collection_opting_into_environment: $(classify "$result")"
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${collection}"
else
    echo "disk_environment_credentials_off: accepted"
    echo "disk_no_credentials: refused_for_credentials"
    echo "collection_opting_into_environment: refused_for_credentials"
fi
