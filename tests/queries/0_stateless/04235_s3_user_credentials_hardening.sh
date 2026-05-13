#!/usr/bin/env bash
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
NC_ROLE_OK="s3_role_ok_${DB}"
NC_NOSIGN="s3_nosign_${DB}"
NC_ENV_OFF="s3_env_off_${DB}"

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
        DROP NAMED COLLECTION IF EXISTS ${NC_ROLE_OK};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOSIGN};
        DROP NAMED COLLECTION IF EXISTS ${NC_ENV_OFF};
    " > /dev/null
}

trap cleanup EXIT
cleanup

# Run `query` and emit `label: pass` only if the server did NOT respond with
# `ACCESS_DENIED`. Any other failure mode (network error, parse error, etc.)
# is acceptable here: this assertion is only about the hardening check.
expect_no_access_denied() {
    local label="$1"
    local query="$2"
    local out
    out="$($CLICKHOUSE_CLIENT --send_logs_level=fatal -q "${query}" 2>&1 || true)"
    if echo "$out" | grep -q "ACCESS_DENIED"; then
        echo "${label}: fail (got ACCESS_DENIED)"
    else
        echo "${label}: pass"
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
expect_no_access_denied "nc_env_off" "
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
expect_no_access_denied "nc_role_with_keys" "
    DESCRIBE TABLE s3(${NC_ROLE_OK}, format = 'TSV', structure = 'x UInt8')
"

# NOSIGN bypasses credential resolution entirely.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        no_sign_request = 1;
" > /dev/null
expect_no_access_denied "nc_nosign" "
    DESCRIBE TABLE s3(${NC_NOSIGN}, format = 'TSV', structure = 'x UInt8')
"

# `extra_credentials(role_arn=...)` is allowed when keys are also supplied.
expect_no_access_denied "extra_credentials_with_keys" "
    DESCRIBE TABLE s3('http://localhost:11111/test/${DB}.tsv', 'k', 's',
                      format = 'TSV', structure = 'x UInt8',
                      extra_credentials(role_arn = '${ROLE_ARN}'))
"
