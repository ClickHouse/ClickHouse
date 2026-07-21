#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# Verify that a dynamic GCS disk created from user SQL cannot use `service_account_key_file` to make the
# server read an arbitrary local file path supplied by the query: `getDiskConfigurationFromAST.cpp` must
# reject it outright (this check does not depend on `USE_GOOGLE_CLOUD`, since it runs on the AST before the
# disk is handed to the object storage factory). The rejection must also cover an indirect value supplied
# via `from_env`/`from_zk` (which still resolves to a server-side path) and a backend whose type is supplied
# indirectly and could resolve to `gcs`. The other native credential fields (`service_account_key`,
# `access_token`, `google_adc_*`) must likewise be refused when supplied indirectly, since the placeholder
# resolves to server-managed auth material. An inline `service_account_key` (not a path) must still be
# allowed by this check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="gcs_key_file_${DB}"
DISK="gcs_key_file_disk_${DB}"

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_a', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_a/',
        service_account_key_file = '/etc/passwd'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_b', type = object_storage, object_storage_type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_b/',
        service_account_key_file = '/etc/passwd'); -- { serverError ACCESS_DENIED }
"

# The rejection must not be bypassable with an indirect value. `service_account_key_file = from_env ...` /
# `from_zk ...` still resolves to a server-side path the backend opens, so with `dynamic_disk_allow_from_env`
# / `dynamic_disk_allow_from_zk` enabled the field must still be refused (the check fires on the AST, before
# any environment/ZooKeeper resolution). An indirect backend type that could resolve to `gcs` is treated
# conservatively as potentially GCS, the same way the S3 restriction treats indirect types.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 --dynamic_disk_allow_from_zk=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_env', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_env/',
        service_account_key_file = 'from_env ${DB}_SA_KEY_FILE'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_zk', type = object_storage, object_storage_type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_zk/',
        service_account_key_file = 'from_zk /${DB}/sa_key_file'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_indirect_type', type = object_storage,
        object_storage_type = 'from_env ${DB}_OST',
        endpoint = 'https://storage.googleapis.com/${DB}_it/',
        service_account_key_file = '/etc/passwd'); -- { serverError ACCESS_DENIED }
"

# The other native GCS credential fields (service_account_key, access_token, google_adc_*) are accepted as
# literals but must be refused when supplied indirectly: a from_env/from_zk placeholder resolves on the
# server, so the disk would authenticate the user query with server-managed auth material (an environment
# secret or a ZooKeeper node). Like the cases above, these are rejected on the AST, before any object
# storage is created, so they are safe in every build type.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 --dynamic_disk_allow_from_zk=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_key_env', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_ke/',
        service_account_key = 'from_env ${DB}_SA_KEY'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_token_zk', type = object_storage, object_storage_type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_tz/',
        access_token = 'from_zk /${DB}/access_token'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_adc_env', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_ae/',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret',
        google_adc_refresh_token = 'from_env ${DB}_ADC_RT'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_key_indirect_type', type = object_storage,
        object_storage_type = 'from_env ${DB}_OST2',
        endpoint = 'https://storage.googleapis.com/${DB}_ki/',
        service_account_key = 'from_env ${DB}_SA_KEY2'); -- { serverError ACCESS_DENIED }
"

# `include` resolves named nodes from the server-side include file only after these checks run, so an
# `include`d disk that is not provably non-GCS is treated as potentially GCS: the included configuration
# could supply `type = gcs`, making the server open the `service_account_key_file` from the AST, or resolve
# indirect credential placeholders. These are rejected on the AST (before the include is even resolved), so
# they are safe in every build type. The complementary case -- the key file or credential fields coming from
# the included configuration itself -- is guarded post-resolution by `validateResolvedGCSDiskCredentials`,
# which needs a real include target and is exercised by the code path shared with the S3 re-check.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_include=1 --dynamic_disk_allow_from_env=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_incl', include = '${DB}_gcs_disk',
        service_account_key_file = '/etc/passwd'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_incl_key', include = '${DB}_gcs_disk2',
        service_account_key = 'from_env ${DB}_SA_KEY3'); -- { serverError ACCESS_DENIED }
"

# Positive control: an inline `service_account_key` (not a path) must not be rejected by this check. Unlike
# the two negative cases above (which are rejected on the AST, before any object storage is created), this
# config passes the check and proceeds to construct the native GCS backend. Table creation then still fails
# for an unrelated reason (no live GCS endpoint here), but that failure must not be `ACCESS_DENIED`.
#
# The construction of the native backend parses JSON through the shared `contrib/nlohmann-json`, whose lexer
# and serializer call `localeconv`, which is trapped by `base/harmful/harmful.c` in any `DEBUG_OR_SANITIZER_BUILD`.
# Reaching that path there aborts the server (SIGILL) and takes unrelated tests in the same shard down with it,
# so run this positive control only in release-type builds. The rejection logic it guards is build-independent
# AST processing (`getDiskConfigurationFromAST.cpp`) fully covered by release/coverage/arm CI runs.
debug_or_sanitizer=$($CLICKHOUSE_CLIENT -q "
    SELECT (SELECT value FROM system.build_options WHERE name = 'BUILD_TYPE') = 'Debug'
        OR (SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS') LIKE '%sanitize%'")

if [ "${debug_or_sanitizer}" = "1" ]; then
    echo "inline_key: pass"
else
    out="$($CLICKHOUSE_CLIENT -q "
        CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(name = '${DISK}_c', type = gcs,
            endpoint = 'https://storage.googleapis.com/${DB}_c/',
            service_account_key = '{}', skip_access_check = 1)" 2>&1)"
    if echo "${out}" | grep -q "(ACCESS_DENIED)"; then
        echo "inline_key: fail (${out//$'\n'/ })"
    else
        echo "inline_key: pass"
    fi
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
