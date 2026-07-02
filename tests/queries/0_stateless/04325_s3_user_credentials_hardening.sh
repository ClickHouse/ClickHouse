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
#
# To keep the test fast under sanitizer builds (where each `clickhouse-client`
# process start is expensive), independent statements are grouped into as few
# client invocations as possible: all named collections are created at once, and
# the negative cases are batched into single multi-query calls using per-query
# `-- { serverError ... }` hints (each hint is validated independently).

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
NC_BACKUP_GCP_ADC="s3_backup_gcp_adc_${DB}"
NC_GCP_OAUTH="s3_gcp_oauth_${DB}"
NC_GCP_OAUTH_NOSIGN="s3_gcp_oauth_nosign_${DB}"
NC_GCP_OAUTH_CASE="s3_gcp_oauth_case_${DB}"
NC_NOCREDS="s3_nocreds_${DB}"
NC_NOSIGN_ROLE="s3_nosign_role_${DB}"
NC_GCP_ADC="s3_gcp_adc_${DB}"
NC_GCP_ADC_ROLE="s3_gcp_adc_role_${DB}"

TABLE="s3_hardening_${DB}"
DISK="s3_hardening_disk_${DB}"

ROLE_ARN="arn:aws:iam::123456789012:role/Test_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS ${TABLE};
        DROP TABLE IF EXISTS ${TABLE}_anon;
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
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_GCP_ADC};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH_NOSIGN};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_OAUTH_CASE};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOCREDS};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOSIGN_ROLE};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_ADC};
        DROP NAMED COLLECTION IF EXISTS ${NC_GCP_ADC_ROLE};
    " > /dev/null
}

trap cleanup EXIT
cleanup

# ----------------------------------------------------------------------------
# Create every named collection up front (one client invocation).
# ----------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_ENV} AS
        url = 'http://localhost:11111/test/${DB}.tsv', use_environment_credentials = 1;
    CREATE NAMED COLLECTION ${NC_ROLE} AS
        url = 'http://localhost:11111/test/${DB}.tsv', role_arn = '${ROLE_ARN}';
    CREATE NAMED COLLECTION ${NC_ROLE_PARTIAL} AS
        url = 'http://localhost:11111/test/${DB}.tsv', access_key_id = 'k', role_arn = '${ROLE_ARN}';
    CREATE NAMED COLLECTION ${NC_GCP_OAUTH} AS
        url = 'http://localhost:11111/test/${DB}.tsv', http_client = 'gcp_oauth';
    CREATE NAMED COLLECTION ${NC_GCP_OAUTH_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}.tsv', http_client = 'gcp_oauth', no_sign_request = 1;
    CREATE NAMED COLLECTION ${NC_GCP_OAUTH_CASE} AS
        url = 'http://localhost:11111/test/${DB}.tsv', http_client = 'GCP_OAUTH';
    CREATE NAMED COLLECTION ${NC_NOCREDS} AS
        url = 'http://localhost:11111/test/${DB}_missing.tsv';
    CREATE NAMED COLLECTION ${NC_NOSIGN_ROLE} AS
        url = 'http://localhost:11111/test/${DB}_missing.tsv', no_sign_request = 1, role_arn = '${ROLE_ARN}';
    CREATE NAMED COLLECTION ${NC_BACKUP_ROLE} AS
        url = 'http://localhost:11111/test/${DB}_backup3/', role_arn = '${ROLE_ARN}';
    CREATE NAMED COLLECTION ${NC_BACKUP_ENV} AS
        url = 'http://localhost:11111/test/${DB}_backup4/', use_environment_credentials = 1;
    CREATE NAMED COLLECTION ${NC_ENV_OFF} AS
        url = 'http://localhost:11111/test/${DB}.tsv', access_key_id = 'k', secret_access_key = 's', use_environment_credentials = 0;
    CREATE NAMED COLLECTION ${NC_ROLE_OK} AS
        url = 'http://localhost:11111/test/${DB}.tsv', access_key_id = 'k', secret_access_key = 's', role_arn = '${ROLE_ARN}';
    CREATE NAMED COLLECTION ${NC_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}.tsv', no_sign_request = 1;
    CREATE NAMED COLLECTION ${NC_BACKUP_NOSIGN} AS
        url = 'http://localhost:11111/test/${DB}_backup_nosign/', no_sign_request = 1;
    CREATE NAMED COLLECTION ${NC_BACKUP_NOCREDS} AS
        url = 'http://localhost:11111/test/${DB}_backup_nocreds/';
    CREATE NAMED COLLECTION ${NC_BACKUP_GCP_ADC} AS
        url = 'http://localhost:11111/test/${DB}_backup_gcp_adc/', http_client = 'gcp_oauth',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token';
    CREATE NAMED COLLECTION ${NC_GCP_ADC} AS
        url = 'http://localhost:11111/test/${DB}.tsv', http_client = 'gcp_oauth',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token';
    CREATE NAMED COLLECTION ${NC_GCP_ADC_ROLE} AS
        url = 'http://localhost:11111/test/${DB}.tsv', http_client = 'gcp_oauth',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token',
        role_arn = '${ROLE_ARN}';
" > /dev/null

# ----------------------------------------------------------------------------
# Negative cases: must be rejected with ACCESS_DENIED when credentials would be
# resolved from the server environment / IMDS / STS. Anonymous fallbacks that
# legitimately reach S3 must instead fail with S3_ERROR, not ACCESS_DENIED.
# All batched into one call; each `-- { serverError ... }` hint is validated
# independently.
#
#  * ${NC_ENV}            - use_environment_credentials, no explicit keys
#  * ${NC_ROLE}           - role_arn, no explicit S3 credentials
#  * ${NC_ROLE_PARTIAL}   - role_arn with only access_key_id (half a key pair)
#  * ${NC_GCP_OAUTH}      - http_client = gcp_oauth (server GCP metadata token)
#  * ${NC_GCP_OAUTH_NOSIGN} - gcp_oauth still mints a token even with NOSIGN
#  * ${NC_GCP_OAUTH_CASE} - http client name matched case-insensitively
#  * extra_credentials    - s3() with extra_credentials(role_arn) and no keys: the query-supplied role_arn is
#                           dropped (it must not assume the role with the server's keys), so the request goes
#                           anonymous and reaches S3 (S3_ERROR), it is not a server-credential rejection
#  * ${NC_NOCREDS}        - url only -> anonymous, reaches S3 (S3_ERROR)
#  * ${NC_NOSIGN_ROLE}    - NOSIGN with a stray role_arn stays anonymous (S3_ERROR)
$CLICKHOUSE_CLIENT -m -q "
    SELECT * FROM s3(${NC_ENV}, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
    SELECT * FROM s3(${NC_ROLE}, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
    SELECT * FROM s3(${NC_ROLE_PARTIAL}, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
    SELECT * FROM s3('http://localhost:11111/test/${DB}.tsv', format = 'TSV', structure = 'x UInt8',
                     extra_credentials(role_arn = '${ROLE_ARN}')); -- { serverError S3_ERROR }
    SELECT * FROM s3(${NC_GCP_OAUTH}, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
    SELECT * FROM s3(${NC_GCP_OAUTH_NOSIGN}, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
    SELECT * FROM s3(${NC_GCP_OAUTH_CASE}, format = 'TSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
    SELECT * FROM s3(${NC_NOCREDS}, format = 'TSV', structure = 'x UInt8'); -- { serverError S3_ERROR }
    SELECT * FROM s3(${NC_NOSIGN_ROLE}, format = 'TSV', structure = 'x UInt8'); -- { serverError S3_ERROR }
"

# `disk(...)` negative cases in CREATE TABLE. Batched (each fails before the table
# is created). They run before the dummy table below so the failure is the disk
# credential rejection, not a "table already exists" error.
#  * _a   - disk(type=s3, use_environment_credentials=1)
#  * _b   - disk(object_storage_type=s3, use_environment_credentials=1)
#  * _c   - disk(type=s3, role_arn=...) with no explicit keys
#  * _d   - disk(type=s3, ...) with no credentials at all
#  * _gcp - gcp_oauth ignores S3 keys; explicit keys are not a substitute for an ADC triple
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_a', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_a/', use_environment_credentials = 1); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_b', type = object_storage, object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_b/', use_environment_credentials = 1); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_c', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_c/', role_arn = '${ROLE_ARN}'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_d', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_d/'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_gcp', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_gcp/', http_client = 'gcp_oauth',
        access_key_id = 'k', secret_access_key = 's'); -- { serverError ACCESS_DENIED }
"

# `from_env` placeholders resolve to the server's environment, so they must not
# satisfy the explicit-credentials requirement even with dynamic_disk_allow_from_env.
# A `from_env` disk type could hide an S3 disk, so it is treated as S3 and rejected.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_from_env=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_env', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_env/',
        access_key_id = 'from_env ${DB}_AKID', secret_access_key = 'from_env ${DB}_SAK'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_indirect_type', type = 'from_env ${DB}_DISK_TYPE',
        endpoint = 'http://localhost:11111/test/${DB}_indirect_type/',
        access_key_id = 'k', secret_access_key = 's'); -- { serverError ACCESS_DENIED }
"

# `include` could pull S3 credentials from server config into a user-created disk.
$CLICKHOUSE_CLIENT --dynamic_disk_allow_include=1 -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_include', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_include/', include = '${DB}_some_disk',
        access_key_id = 'k', secret_access_key = 's')
    -- { serverError ACCESS_DENIED }
"

# Create a dummy table so the `BACKUP` statements reach credential resolution and
# not an earlier "unknown table" error, then batch the negative backups:
#  * named collection with role_arn but no explicit S3 credentials -> server-credential rejection
#  * named collection with use_environment_credentials = 1 -> server-credential rejection
# (The `extra_credentials(role_arn)` backup form is covered by the s3() SELECT case above: a query-supplied
# role_arn with no keys is dropped to anonymous, whose backup write outcome is bucket-policy dependent.)
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple();
    BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_ROLE}); -- { serverError ACCESS_DENIED }
    BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_ENV}); -- { serverError ACCESS_DENIED }
"

# ----------------------------------------------------------------------------
# Positive cases: must NOT be rejected. The `DESCRIBE TABLE` cases use an explicit
# `structure` so the table-function path is exercised (parse, `fromAST`,
# `fromNamedCollection`) without contacting the S3 endpoint.
# ----------------------------------------------------------------------------

#  * nc_env_off              - explicit keys and use_environment_credentials = 0
#  * nc_role_with_keys       - role_arn AND explicit keys (assume-role uses user creds)
#  * nc_nosign               - NOSIGN bypasses credential resolution
#  * extra_credentials_with_keys - extra_credentials(role_arn) allowed with explicit keys
describe_labels="nc_env_off nc_role_with_keys nc_nosign extra_credentials_with_keys"
if describe_out="$($CLICKHOUSE_CLIENT -m -q "
    DESCRIBE TABLE s3(${NC_ENV_OFF}, format = 'TSV', structure = 'x UInt8');
    DESCRIBE TABLE s3(${NC_ROLE_OK}, format = 'TSV', structure = 'x UInt8');
    DESCRIBE TABLE s3(${NC_NOSIGN}, format = 'TSV', structure = 'x UInt8');
    DESCRIBE TABLE s3('http://localhost:11111/test/${DB}.tsv', 'k', 's',
                      format = 'TSV', structure = 'x UInt8', extra_credentials(role_arn = '${ROLE_ARN}'));
" 2>&1)"; then
    for label in $describe_labels; do echo "${label}: pass"; done
else
    describe_out="${describe_out//$'\n'/ }"
    for label in $describe_labels; do echo "${label}: fail (${describe_out})"; done
fi

# Run `query` and emit `label: pass` unless the server-managed-credentials check rejected it. The operation may
# legitimately fail later (contacting S3, minting a token); in particular an anonymous/NOSIGN request to the
# private test bucket gets an S3 `403` whose message carries `error type: ACCESS_DENIED` but the exception code
# `(S3_ERROR)`. Match only the restriction's own exception code `(ACCESS_DENIED)` so that S3 `403`s do not count.
expect_not_denied() {
    local label="$1"
    local out
    out="$($CLICKHOUSE_CLIENT -q "$2" 2>&1)"
    if echo "${out}" | grep -q "(ACCESS_DENIED)"; then
        echo "${label}: fail (${out//$'\n'/ })"
    else
        echo "${label}: pass"
    fi
}

# Backup positives (each may fail contacting S3, but must not be ACCESS_DENIED):
#  * NOSIGN backup is unsigned access
#  * url-only backup defaults use_environment_credentials to 0 -> anonymous
#  * gcp_oauth with a complete explicit Google ADC triple is a user credential
expect_not_denied "backup_nosign" "BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_NOSIGN})"
expect_not_denied "backup_url_only" "BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_NOCREDS})"
expect_not_denied "backup_gcp_adc" "BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_GCP_ADC})"

# `gcp_oauth` with a complete explicit Google ADC triple is a user-supplied credential, so it must be allowed
# (it then fails minting the token from the bogus triple, but is not rejected with ACCESS_DENIED). A stray
# role_arn alongside the ADC triple must not turn it into an STS assume-role.
expect_not_denied "gcp_oauth_adc" "SELECT * FROM s3(${NC_GCP_ADC}, format = 'TSV', structure = 'x UInt8')"
expect_not_denied "gcp_oauth_adc_role" "SELECT * FROM s3(${NC_GCP_ADC_ROLE}, format = 'TSV', structure = 'x UInt8')"

# An explicit anonymous dynamic S3 disk (no server-managed credential) must be allowed (unsigned); it then
# fails contacting S3, not ACCESS_DENIED.
expect_not_denied "disk_anonymous" "
    CREATE TABLE ${TABLE}_anon (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_anon', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_anon/', use_environment_credentials = 0)"

# The `session_token` and Google ADC secret keys, when supplied as named-collection overrides, must be masked
# the same way `secret_access_key` is, so they do not leak into the query log. Run queries carrying them inline
# (the `s3` table function and the cluster form, where the collection is the second argument) and check the
# logged query hides the secret values. The query is masked by the AST formatter, independent of the analyzer.
mask_qid="04325_adc_mask_${DB}_${RANDOM}"
cluster_mask_qid="04325_cluster_mask_${DB}_${RANDOM}"
$CLICKHOUSE_CLIENT --query_id "${mask_qid}" -q "
    SELECT * FROM s3(${NC_NOCREDS},
        session_token = 'SESSION_TOKEN_LEAK_CHECK',
        google_adc_client_secret = 'ADC_SECRET_LEAK_CHECK',
        google_adc_refresh_token = 'ADC_TOKEN_LEAK_CHECK',
        format = 'TSV', structure = 'x UInt8')" > /dev/null 2>&1
$CLICKHOUSE_CLIENT --query_id "${cluster_mask_qid}" -q "
    SELECT * FROM s3Cluster('test_shard_localhost', ${NC_NOCREDS},
        session_token = 'SESSION_TOKEN_LEAK_CHECK',
        google_adc_client_secret = 'ADC_SECRET_LEAK_CHECK',
        google_adc_refresh_token = 'ADC_TOKEN_LEAK_CHECK',
        format = 'TSV', structure = 'x UInt8')" > /dev/null 2>&1

# Check that a logged query hides the secrets and shows HIDDEN.
check_masked() {
    local label="$1" out="$2"
    if echo "${out}" | grep -qE "SESSION_TOKEN_LEAK_CHECK|ADC_SECRET_LEAK_CHECK|ADC_TOKEN_LEAK_CHECK"; then
        echo "${label}: fail (secret leaked: ${out//$'\n'/ })"
    elif echo "${out}" | grep -q "HIDDEN"; then
        echo "${label}: pass"
    else
        echo "${label}: fail (no masked query found: ${out//$'\n'/ })"
    fi
}

adc_mask_out="$($CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query FROM system.query_log
    WHERE query_id = '${mask_qid}' AND current_database = currentDatabase() AND query LIKE '%s3(%'
    ORDER BY event_time_microseconds LIMIT 1")"
check_masked "adc_masking" "${adc_mask_out}"

cluster_mask_out="$($CLICKHOUSE_CLIENT -q "
    SELECT query FROM system.query_log
    WHERE query_id = '${cluster_mask_qid}' AND current_database = currentDatabase() AND query LIKE '%s3Cluster(%'
    ORDER BY event_time_microseconds LIMIT 1")"
check_masked "s3cluster_masking" "${cluster_mask_out}"
