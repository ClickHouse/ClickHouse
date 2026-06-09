#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `s3` table function / `S3` storage / `BACKUP TO S3`,
# which are not compiled into the fast-test build.
#
# Verify that user SQL cannot make S3 clients use the server's environment
# (IMDS/IRSA/STS) credentials, and that user SQL cannot trigger an STS
# AssumeRole without supplying explicit S3 credentials.
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
NC_BACKUP_NOSIGN="s3_backup_nosign_${DB}"
NC_BACKUP_NOCREDS="s3_backup_nocreds_${DB}"
NC_GCP_OAUTH="s3_gcp_oauth_${DB}"
NC_GCP_OAUTH_NOSIGN="s3_gcp_oauth_nosign_${DB}"
NC_GCP_OAUTH_CASE="s3_gcp_oauth_case_${DB}"
NC_NOCREDS="s3_nocreds_${DB}"
NC_NOSIGN_ROLE="s3_nosign_role_${DB}"

TABLE="s3_hardening_${DB}"
DISK="s3_hardening_disk_${DB}"

ROLE_ARN="arn:aws:iam::123456789012:role/Test_${DB}"

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
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_NOSIGN};
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_NOCREDS};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH_NOSIGN};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH_CASE};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOCREDS};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOSIGN_ROLE};
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
    if out="$($CLICKHOUSE_CLIENT -q "${query}" 2>&1)"; then
        echo "${label}: pass"
    else
        out="${out//$'\n'/ }"
        echo "${label}: fail (${out})"
    fi
}

# ----------------------------------------------------------------------------
# Negative cases: must be rejected with ACCESS_DENIED when credentials would be
# resolved from the server environment / IMDS / STS.
# ----------------------------------------------------------------------------

# Named collection with `use_environment_credentials = 1` and no explicit keys.
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

# Named collection with `role_arn` and only `access_key_id` (no secret): one
# half of the key pair is not enough to authorize an STS AssumeRole.
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

# `s3()` with `extra_credentials(role_arn=...)` and no explicit keys.
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3('http://localhost:11111/test/${DB}.tsv', format = 'TSV',
            structure = 'x UInt8',
            extra_credentials(role_arn = '${ROLE_ARN}'))
    -- { serverError ACCESS_DENIED }
"

# Named collection with `http_client = gcp_oauth`, which fetches a bearer token from the server's GCP
# OAuth metadata service. It is server-managed regardless of any S3 keys.
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

# `gcp_oauth` still fetches a server bearer token even with NOSIGN, so NOSIGN must not bypass the check.
# `http_client` is only honored for named collections, so the case is expressed as one.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_GCP_OAUTH_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        http_client = 'gcp_oauth',
        no_sign_request = 1;
"
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_GCP_OAUTH_NOSIGN}, format = 'TSV', structure = 'x UInt8')
    -- { serverError ACCESS_DENIED }
"

# The http client name is matched case-insensitively, so a different case must not bypass the check.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_GCP_OAUTH_CASE} AS
        url = 'http://localhost:11111/test/${DB}.tsv',
        http_client = 'GCP_OAUTH';
"
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_GCP_OAUTH_CASE}, format = 'TSV', structure = 'x UInt8')
    -- { serverError ACCESS_DENIED }
"

# Anonymous fallback: a named collection that only specifies a URL must NOT be refused. It defaults
# `use_environment_credentials` to 0, so the request goes unsigned (anonymous) and reaches S3, failing with
# an S3 error on the missing object rather than ACCESS_DENIED.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_NOCREDS} AS
        url = 'http://localhost:11111/test/${DB}_missing.tsv';
"
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_NOCREDS}, format = 'TSV', structure = 'x UInt8')
    -- { serverError S3_ERROR }
"

# NOSIGN must bypass credential resolution entirely: a stray `role_arn` alongside NOSIGN must not trigger an
# STS assume-role. The request stays anonymous and reaches S3 (S3 error on the missing object), not an STS
# failure or ACCESS_DENIED.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_NOSIGN_ROLE} AS
        url = 'http://localhost:11111/test/${DB}_missing.tsv',
        no_sign_request = 1,
        role_arn = '${ROLE_ARN}';
"
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM s3(${NC_NOSIGN_ROLE}, format = 'TSV', structure = 'x UInt8')
    -- { serverError S3_ERROR }
"

# `disk(type=s3, use_environment_credentials=1)` in CREATE TABLE.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_a',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_a/',
        use_environment_credentials = 1)
    -- { serverError ACCESS_DENIED }
"

# `disk(object_storage_type=s3, use_environment_credentials=1)`.
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

# `disk(type=s3, role_arn=...)` with no explicit keys.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_c',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_c/',
        role_arn = '${ROLE_ARN}')
    -- { serverError ACCESS_DENIED }
"

# `disk(type=s3, ...)` with no credentials at all (no keys, no NOSIGN): would fall back to the server's
# environment / instance-profile / AWS config-file credentials.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_d',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_d/')
    -- { serverError ACCESS_DENIED }
"

# `http_client = gcp_oauth` mints a bearer token from the server's GCP metadata service and ignores any S3
# keys, so an explicit `access_key_id`/`secret_access_key` pair must not be accepted as a substitute for a
# complete explicit Google ADC triple.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_gcp',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_gcp/',
        http_client = 'gcp_oauth',
        access_key_id = 'k',
        secret_access_key = 's')
    -- { serverError ACCESS_DENIED }
"

# `from_env` placeholders are not user-supplied credentials: they resolve to the server's environment, so
# they must not satisfy the explicit-credentials requirement even when dynamic_disk_allow_from_env is on.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_env',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_env/',
        access_key_id = 'from_env ${DB}_AKID',
        secret_access_key = 'from_env ${DB}_SAK')
    -- { serverError ACCESS_DENIED }
"

# A `from_env` disk type could hide an S3 disk, so it is treated as potentially S3 and rejected.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_indirect_type',
        type = 'from_env ${DB}_DISK_TYPE',
        endpoint = 'http://localhost:11111/test/${DB}_indirect_type/',
        access_key_id = 'k',
        secret_access_key = 's')
    -- { serverError ACCESS_DENIED }
"

# `include` could pull S3 credentials from server config into a user-created disk.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_include=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '${DISK}_include',
        type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_include/',
        include = '${DB}_some_disk',
        access_key_id = 'k',
        secret_access_key = 's')
    -- { serverError ACCESS_DENIED }
"

# Create a dummy table so the `BACKUP` statements reach credential resolution
# and not an earlier "unknown table" error.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple();
"

# `BACKUP ... TO S3(url, extra_credentials(role_arn=...))` without explicit keys.
$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE}
    TO S3('http://localhost:11111/test/${DB}_backup1/',
          extra_credentials(role_arn = '${ROLE_ARN}'))
    -- { serverError ACCESS_DENIED }
"

# `BACKUP ... TO S3(named_collection)` where the collection has a `role_arn`
# but no explicit S3 credentials.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_ROLE} AS
        url = 'http://localhost:11111/test/${DB}_backup3/',
        role_arn = '${ROLE_ARN}';
"

$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_ROLE})
    -- { serverError ACCESS_DENIED }
"

# `BACKUP ... TO S3(named_collection)` with `use_environment_credentials = 1`.
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_ENV} AS
        url = 'http://localhost:11111/test/${DB}_backup4/',
        use_environment_credentials = 1;
"

$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_ENV})
    -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE};"

# ----------------------------------------------------------------------------
# Positive cases: must NOT be rejected. We use `DESCRIBE TABLE` with an explicit
# `structure` so the table-function path is fully exercised (parse, `fromAST`,
# `fromNamedCollection`, credential resolution) but the storage never contacts
# the S3 endpoint.
# ----------------------------------------------------------------------------

# Named collection with explicit keys and `use_environment_credentials = 0`.
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

# Named collection with `role_arn` AND explicit S3 keys (assume-role uses the
# user's own credentials, which is allowed).
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

# `extra_credentials(role_arn=...)` is allowed when explicit keys are supplied.
expect_success "extra_credentials_with_keys" "
    DESCRIBE TABLE s3('http://localhost:11111/test/${DB}.tsv', 'k', 's',
                      format = 'TSV', structure = 'x UInt8',
                      extra_credentials(role_arn = '${ROLE_ARN}'))
"

# A backup named collection with NOSIGN is unsigned access, which is allowed. The backup must not be
# rejected by the server-managed-credentials check (it may fail later contacting S3, but not ACCESS_DENIED).
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}_backup_nosign/',
        no_sign_request = 1;
" > /dev/null
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple();"
backup_nosign_out="$($CLICKHOUSE_CLIENT -q "BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_NOSIGN})" 2>&1)"
if echo "${backup_nosign_out}" | grep -q "ACCESS_DENIED"; then
    echo "backup_nosign: fail (${backup_nosign_out//$'\n'/ })"
else
    echo "backup_nosign: pass"
fi

# A backup named collection that only specifies a URL defaults `use_environment_credentials` to 0, so it must
# not authenticate with the server's credentials: the backup goes anonymous and is not rejected with
# ACCESS_DENIED (it may fail later contacting S3).
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_BACKUP_NOCREDS} AS
        url = 'http://localhost:11111/test/${DB}_backup_nocreds/';
" > /dev/null
backup_nocreds_out="$($CLICKHOUSE_CLIENT -q "BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_NOCREDS})" 2>&1)"
if echo "${backup_nocreds_out}" | grep -q "ACCESS_DENIED"; then
    echo "backup_url_only: fail (${backup_nocreds_out//$'\n'/ })"
else
    echo "backup_url_only: pass"
fi
