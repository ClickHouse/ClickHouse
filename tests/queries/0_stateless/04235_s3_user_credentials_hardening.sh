#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `s3` table function / `S3` storage / `BACKUP TO S3`,
# which are not compiled into the fast-test build.
#
# Verify that user SQL cannot make S3 clients use the server's environment
# (IMDS/IRSA/STS) credentials, and that user SQL cannot trigger STS AssumeRole
# without supplying explicit S3 credentials.
#
# All globally-scoped names (named collections, table names, disk names) are
# qualified with `$CLICKHOUSE_DATABASE` so that several invocations of this
# test can run in parallel without colliding on shared state.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

NC_ENV="s3_env_${DB}"
NC_ROLE="s3_role_${DB}"
NC_ROLE_PARTIAL="s3_role_partial_${DB}"
NC_ROLE_OK="s3_role_ok_${DB}"
NC_NOSIGN="s3_nosign_${DB}"
NC_ENV_OFF="s3_env_off_${DB}"
NC_BACKUP_ROLE="s3_backup_role_${DB}"
NC_BACKUP_ENV="s3_backup_env_${DB}"
NC_BACKUP_NO_OVERRIDE="s3_backup_no_override_${DB}"
NC_GCP_OAUTH="s3_gcp_oauth_${DB}"

TABLE="s3_hardening_${DB}"
DISK="s3_hardening_disk_${DB}"

# Construct an ARN-looking literal at runtime so that the `S3Exception` AWS-ARN
# sanitizer cannot redact this text when it appears in our test output.
ROLE_ARN="$(printf 'arn:aws:iam::123456789012:role/Test_%s' "${DB}")"

cleanup() {
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS ${TABLE};
        DROP NAMED COLLECTION IF EXISTS ${NC_ENV};
        DROP NAMED COLLECTION IF EXISTS ${NC_ROLE};
        DROP NAMED COLLECTION IF EXISTS ${NC_ROLE_PARTIAL};
        DROP NAMED COLLECTION IF EXISTS ${NC_ROLE_OK};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOSIGN};
        DROP NAMED COLLECTION IF EXISTS ${NC_ENV_OFF};
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_ROLE};
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_ENV};
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_NO_OVERRIDE};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH};
    " > /dev/null
}

trap cleanup EXIT
cleanup

# Run `query` and emit `label: pass` only if the allowed form executes
# successfully. The positive cases use `DESCRIBE TABLE` with an explicit
# structure, so they should be local and should not contact S3.
expect_success() {
    local label="$1"
    local query="$2"
    local out
    if out="$($CLICKHOUSE_CLIENT --send_logs_level=fatal -q "${query}" 2>&1)"; then
        echo "${label}: pass"
    else
        out="${out//$'\n'/ }"
        echo "${label}: fail (${out})"
    fi
}

# ----------------------------------------------------------------------------
# Negative cases: must be rejected at parse / configuration time.
# These return immediately with ACCESS_DENIED (or BAD_ARGUMENTS) and do not
# contact S3.
# ----------------------------------------------------------------------------

# `s3()` with `use_environment_credentials` as a key-value argument.
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3('http://localhost:11111/test/${DB}.tsv', format = 'TSV',
            structure = 'x UInt8', use_environment_credentials = 1)
    -- { serverError BAD_ARGUMENTS }
"

# `s3()` with `extra_credentials(role_arn=...)` and no explicit keys.
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3('http://localhost:11111/test/${DB}.tsv', format = 'TSV',
            structure = 'x UInt8',
            extra_credentials(role_arn = '${ROLE_ARN}'))
    -- { serverError ACCESS_DENIED }
"

# Named collection with `use_environment_credentials = 1`.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_ENV} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        use_environment_credentials = 1;
"

$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_ENV}, format = 'TSV', structure = 'x UInt8')
    -- { serverError ACCESS_DENIED }
"

# Named collection with `gcp_oauth` but no explicit ADC credentials.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_GCP_OAUTH} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        http_client = 'gcp_oauth';
"

$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_GCP_OAUTH}, format = 'TSV', structure = 'x UInt8')
    -- { serverError ACCESS_DENIED }
"

# Named collection with `role_arn` and no explicit S3 credentials.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_ROLE} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        role_arn = '${ROLE_ARN}';
"

$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_ROLE}, format = 'TSV', structure = 'x UInt8')
    -- { serverError ACCESS_DENIED }
"

# Named collection with `role_arn` and only `access_key_id` (no secret).
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_ROLE_PARTIAL} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        access_key_id = 'k',
        role_arn = '${ROLE_ARN}';
"

$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_ROLE_PARTIAL}, format = 'TSV', structure = 'x UInt8')
    -- { serverError ACCESS_DENIED }
"

# `s3()` with `extra_credentials(role_arn=...)` and a positional `access_key_id`
# but an empty `secret_access_key`. The user is supplying only one half of the
# key pair, which must not be sufficient to authorize an STS `AssumeRole`.
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3('http://localhost:11111/test/${DB}.tsv', 'k', '',
            format = 'TSV', structure = 'x UInt8',
            extra_credentials(role_arn = '${ROLE_ARN}'))
    -- { serverError ACCESS_DENIED }
"

# `disk()` for direct S3 type with `use_environment_credentials = 1`.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_a',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_a/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk()` for object_storage / object_storage_type = s3 with env credentials.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_b',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_b/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk()` for s3_plain (must also be blocked).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_c',
        type = object_storage,
        object_storage_type = s3_plain,
        endpoint = 'http://localhost:11111/test/${DB}_c/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk()` for s3_with_keeper.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_d',
        type = object_storage,
        object_storage_type = s3_with_keeper,
        endpoint = 'http://localhost:11111/test/${DB}_d/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk()` for s3_plain_rewritable.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_e',
        type = object_storage,
        object_storage_type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/${DB}_e/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk()` with `role_arn` but no explicit keys.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_f',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_f/',
        role_arn = '${ROLE_ARN}')
    -- { serverError ACCESS_DENIED }
"

# `disk()` with `role_arn` and only `access_key_id` (missing secret).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_g',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_g/',
        access_key_id = 'key',
        role_arn = '${ROLE_ARN}')
    -- { serverError ACCESS_DENIED }
"

# `disk()` where `type` is supplied indirectly via `from_env` must still be
# treated as potentially S3, so `use_environment_credentials = 1` must be
# rejected even though the literal `s3` token never appears here.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_env',
        type = 'from_env ${DB}_DISK_TYPE',
        endpoint = 'http://localhost:11111/test/${DB}_indirect_env/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# Same as above for `from_zk` indirection on `type`.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_zk=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_zk',
        type = 'from_zk /${DB}/disk_type',
        endpoint = 'http://localhost:11111/test/${DB}_indirect_zk/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# Same as above for `from_env` indirection on `object_storage_type`.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_obj',
        type = object_storage,
        object_storage_type = 'from_env ${DB}_OST',
        endpoint = 'http://localhost:11111/test/${DB}_indirect_obj/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk()` with an `include` directive could pull in S3 keys from an XML
# config we cannot inspect here, so the credential checks must still apply.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_include=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_include',
        include = '${DB}_some_disk',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk(... type = s3 ..., role_arn = ..., access_key_id = 'from_env X',
# secret_access_key = 'from_env Y')` — the supposed "authorizing" keys are
# themselves indirect, so they must NOT count as user-supplied credentials.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_role_indirect_keys',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_role_indirect_keys/',
        access_key_id = 'from_env ${DB}_AKID',
        secret_access_key = 'from_env ${DB}_SAK',
        role_arn = '${ROLE_ARN}')
    -- { serverError ACCESS_DENIED }
"

# `from_env` credentials must also be rejected when no `role_arn` is present;
# otherwise user-created S3 disk metadata could inherit server environment keys.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_keys',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_indirect_keys/',
        access_key_id = 'from_env ${DB}_AKID',
        secret_access_key = 'from_env ${DB}_SAK')
    -- { serverError ACCESS_DENIED }
"

# Same for `from_zk` credential indirection.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_zk=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_zk_keys',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_zk_keys/',
        access_key_id = 'from_zk /${DB}/akid',
        secret_access_key = 'from_zk /${DB}/sak')
    -- { serverError ACCESS_DENIED }
"

# Header keys are accepted by prefix in `S3AuthSettings`, so dynamic S3 disks
# must reject indirect values for numbered forms too.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_header',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_indirect_header/',
        header1 = 'from_env ${DB}_HEADER')
    -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_access_header',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_indirect_access_header/',
        access_header1 = 'from_env ${DB}_ACCESS_HEADER')
    -- { serverError ACCESS_DENIED }
"

# `gcp_oauth` without an explicit ADC credential triple would use server metadata.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_gcp_oauth',
        type = object_storage,
        object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_gcp_oauth/',
        http_client = 'gcp_oauth')
    -- { serverError ACCESS_DENIED }
"

# `include` could supply credential-bearing S3 fields from server config, so it
# is not allowed for newly created dynamic S3 disk metadata.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_include=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_s3_include',
        type = object_storage,
        object_storage_type = s3,
        include = '${DB}_some_s3_disk',
        endpoint = 'http://localhost:11111/test/${DB}_s3_include/')
    -- { serverError ACCESS_DENIED }
"

# Nested disk(cache, disk = disk(... s3 + env)) must also be rejected
# (the inner disk function is flattened recursively).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_cache',
        type = cache,
        max_size = '1Mi',
        path = 'cache_${DB}/',
        disk = disk(
            name = '${DISK}_inner',
            type = object_storage,
            object_storage_type = s3,
            endpoint = 'http://localhost:11111/test/${DB}_inner/',
            use_environment_credentials = 1))
    -- { serverError ACCESS_DENIED }
"

# Create a dummy table so the `BACKUP` statements reach our argument-parsing
# checks and not an earlier "unknown table" error.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple();
"

# `BACKUP ... TO S3(url, extra_credentials(role_arn = ...))` with no positional
# `access_key_id`/`secret_access_key` must be rejected.
$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE}
    TO S3('http://localhost:11111/test/${DB}_backup1/',
          extra_credentials(role_arn = '${ROLE_ARN}'))
    -- { serverError ACCESS_DENIED }
"

# `BACKUP ... TO S3(url, key, '', extra_credentials(role_arn = ...))` with only
# half of the key pair must also be rejected.
$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE}
    TO S3('http://localhost:11111/test/${DB}_backup2/', 'k', '',
          extra_credentials(role_arn = '${ROLE_ARN}'))
    -- { serverError ACCESS_DENIED }
"

# `BACKUP ... TO S3(named_collection)` where the collection has a `role_arn`
# but no `access_key_id`/`secret_access_key`.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_ROLE} AS
        url = 'http://localhost:11111/test/${DB}_backup3/',
        role_arn = '${ROLE_ARN}';
"

$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_ROLE})
    -- { serverError ACCESS_DENIED }
"

# `BACKUP ... TO S3(named_collection)` where the collection has
# `use_environment_credentials = 1`.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_ENV} AS
        url = 'http://localhost:11111/test/${DB}_backup4/',
        use_environment_credentials = 1;
"

$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_ENV})
    -- { serverError ACCESS_DENIED }
"

# `BACKUP ... TO S3(named_collection, url = ...)` must respect named-collection
# override rules; otherwise a user could redirect operator credentials to a
# different destination.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_NO_OVERRIDE} AS
        url = 'http://localhost:11111/test/${DB}_backup5/' NOT OVERRIDABLE,
        access_key_id = 'k',
        secret_access_key = 's';
"

$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE}
    TO S3(${NC_BACKUP_NO_OVERRIDE}, url = 'http://localhost:11111/test/${DB}_backup6/')
    -- { serverError BAD_ARGUMENTS }
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE};"

# ----------------------------------------------------------------------------
# Positive cases: must NOT be rejected with ACCESS_DENIED. We use `DESCRIBE
# TABLE` with an explicit `structure` so the table function path is fully
# exercised (parse, `fromAST`, `fromNamedCollection`, hardening checks) but
# the storage never tries to contact the S3 endpoint.
# ----------------------------------------------------------------------------

# Named collection with `use_environment_credentials = 0` and explicit keys.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_ENV_OFF} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        access_key_id = 'k',
        secret_access_key = 's',
        use_environment_credentials = 0;
" > /dev/null
expect_success "nc_env_off" "
    DESCRIBE TABLE s3(${NC_ENV_OFF}, format = 'TSV', structure = 'x UInt8')
"

# Named collection with `role_arn` AND explicit S3 keys.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_ROLE_OK} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        access_key_id = 'k',
        secret_access_key = 's',
        role_arn = '${ROLE_ARN}';
" > /dev/null
expect_success "nc_role_with_keys" "
    DESCRIBE TABLE s3(${NC_ROLE_OK}, format = 'TSV', structure = 'x UInt8')
"

# NOSIGN bypasses credential resolution entirely.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        no_sign_request = 1;
" > /dev/null
expect_success "nc_nosign" "
    DESCRIBE TABLE s3(${NC_NOSIGN}, format = 'TSV', structure = 'x UInt8')
"

# `extra_credentials(role_arn=...)` is allowed when keys are also supplied.
expect_success "extra_credentials_with_keys" "
    DESCRIBE TABLE s3('http://localhost:11111/test/${DB}.tsv', 'k', 's',
                      format = 'TSV', structure = 'x UInt8',
                      extra_credentials(role_arn = '${ROLE_ARN}'))
"
