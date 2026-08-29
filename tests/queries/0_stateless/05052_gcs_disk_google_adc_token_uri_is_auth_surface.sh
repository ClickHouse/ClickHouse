#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# `google_adc_token_uri` decides where the refresh token of a native GCS disk is POSTed, so it belongs to
# the disk's authentication surface even though it carries no secret of its own: a server-side value could
# redirect an SQL-supplied refresh token to an endpoint the query never named. It must therefore be refused
# exactly like the `google_adc_*` triple when it is supplied indirectly via `from_env`/`from_zk`, or through
# an `include`. Those refusals are decided on the AST, before the disk reaches the object storage factory,
# so they hold in every build type regardless of `USE_GOOGLE_CLOUD`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="gcs_token_uri_${DB}"
DISK="gcs_token_uri_disk_${DB}"

$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 --dynamic_disk_allow_from_zk=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_env', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_env/',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret',
        google_adc_refresh_token = 'token',
        google_adc_token_uri = 'from_env ${DB}_TOKEN_URI'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_zk', type = object_storage, object_storage_type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_zk/',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret',
        google_adc_refresh_token = 'token',
        google_adc_token_uri = 'from_zk /${DB}/token_uri'); -- { serverError ACCESS_DENIED }
"

# A disk whose backend an `include` could still make `gcs` is treated conservatively as GCS, so the
# indirect token endpoint is refused there too.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_include=1 --dynamic_disk_allow_from_env=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_incl', include = '${DB}_gcs_disk',
        google_adc_token_uri = 'from_env ${DB}_TOKEN_URI2'); -- { serverError ACCESS_DENIED }
"

# Positive control: a literal token endpoint next to a literal triple is the query's own choice and must be
# accepted. The disk is declared `readonly` so that creating the table writes nothing to it, which keeps
# this off the network -- an unreachable endpoint would otherwise be a retryable error the SDK's default
# retry policy would sit on. Only runs where the native backend exists, since it is the one case that
# reaches the object storage factory.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)
if [ "$native_gcs_available" = "1" ]; then
    out="$($CLICKHOUSE_CLIENT --use_native_gcs=1 -q "
        CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(name = '${DISK}_literal', type = gcs,
            endpoint = 'https://storage.googleapis.com/${DB}_literal/',
            google_adc_client_id = 'id', google_adc_client_secret = 'secret',
            google_adc_refresh_token = 'token',
            google_adc_token_uri = 'https://oauth2.example/token',
            readonly = 1, skip_access_check = 1)" 2>&1)"
    if [ -n "${out}" ]; then
        echo "literal_token_uri: fail"
    else
        echo "literal_token_uri: pass"
    fi
else
    echo "literal_token_uri: pass"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
