#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `s3` table function / `S3` storage / `BACKUP TO S3`,
# which are not compiled into the fast-test build.
#
# `google_service_account_key` is an explicit user credential for `http_client = gcp_oauth`, like the
# Google ADC triple: it is accepted from named collections, backup named collections and dynamic S3 disks
# without falling back to the server's credentials, and it is masked like the other S3 secrets.
# The key below is bogus and its `token_uri` points to a closed port, so every positive case fails later,
# while minting the token, but must not be rejected with ACCESS_DENIED.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
NC_SA="s3_gcp_sa_${DB}"
NC_BACKUP_SA="s3_backup_gcp_sa_${DB}"
NC_NOCREDS="s3_nocreds_sa_${DB}"
TABLE="t_05242"
DISK="disk_05242_${DB}"
KEY='{"type": "service_account", "client_email": "a@b.iam.gserviceaccount.com", "private_key": "x", "token_uri": "http://localhost:1/token"}'

cleanup() {
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS ${TABLE};
        DROP TABLE IF EXISTS ${TABLE}_disk;
        DROP TABLE IF EXISTS ${TABLE}_disk_nocreds;
        DROP TABLE IF EXISTS ${TABLE}_engine;
        DROP NAMED COLLECTION IF EXISTS ${NC_SA};
        DROP NAMED COLLECTION IF EXISTS ${NC_BACKUP_SA};
        DROP NAMED COLLECTION IF EXISTS ${NC_NOCREDS};
    " > /dev/null
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_SA} AS
        url = 'http://localhost:11111/test/${DB}_sa.tsv', http_client = 'gcp_oauth', google_service_account_key = '${KEY}';
    CREATE NAMED COLLECTION ${NC_BACKUP_SA} AS
        url = 'http://localhost:11111/test/${DB}_backup_sa/', http_client = 'gcp_oauth', google_service_account_key = '${KEY}';
    CREATE NAMED COLLECTION ${NC_NOCREDS} AS
        url = 'http://localhost:11111/test/${DB}_nocreds.tsv';
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO ${TABLE} VALUES (1);
" > /dev/null

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

expect_denied() {
    local label="$1"
    local out
    out="$($CLICKHOUSE_CLIENT -q "$2" 2>&1)"
    if echo "${out}" | grep -q "(ACCESS_DENIED)"; then
        echo "${label}: pass"
    else
        echo "${label}: fail (${out//$'\n'/ })"
    fi
}

expect_not_denied "named_collection" "SELECT * FROM s3(${NC_SA}, format = 'TSV', structure = 'x UInt8')"
expect_not_denied "backup_named_collection" "BACKUP TABLE ${TABLE} TO S3(${NC_BACKUP_SA})"
expect_not_denied "dynamic_disk" "
    CREATE TABLE ${TABLE}_disk (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}', type = s3, endpoint = 'http://localhost:11111/test/${DB}_disk/',
        http_client = 'gcp_oauth', google_service_account_key = '${KEY}')"
# Control: the same disk without the key would mint a token with the server's GCP identity.
expect_denied "dynamic_disk_without_key" "
    CREATE TABLE ${TABLE}_disk_nocreds (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DISK}_nocreds', type = s3, endpoint = 'http://localhost:11111/test/${DB}_disk_nocreds/',
        http_client = 'gcp_oauth')"

# The key is masked in the logged query (named-collection override) and in SHOW CREATE (explicit-url engine form).
mask_qid="05242_mask_${DB}_${RANDOM}"
$CLICKHOUSE_CLIENT --query_id "${mask_qid}" -q "
    SELECT * FROM s3(${NC_NOCREDS}, google_service_account_key = 'SA_KEY_LEAK_CHECK', format = 'TSV', structure = 'x UInt8')
" > /dev/null 2>&1

check_masked() {
    local label="$1" out="$2"
    if echo "${out}" | grep -q "SA_KEY_LEAK_CHECK"; then
        echo "${label}: fail (secret leaked: ${out//$'\n'/ })"
    elif echo "${out}" | grep -q "HIDDEN"; then
        echo "${label}: pass"
    else
        echo "${label}: fail (no masked query found: ${out//$'\n'/ })"
    fi
}

check_masked "query_log_masking" "$($CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query FROM system.query_log
    WHERE query_id = '${mask_qid}' AND current_database = currentDatabase() AND query LIKE '%s3(%'
    ORDER BY event_time_microseconds LIMIT 1")"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE}_engine (x UInt8)
    ENGINE = S3('http://localhost:11111/test/${DB}_engine.tsv', 'ak', 'sak', google_service_account_key = 'SA_KEY_LEAK_CHECK', format = 'TSV')
" > /dev/null
check_masked "show_create_masking" "$($CLICKHOUSE_CLIENT -q "
    SHOW CREATE TABLE ${TABLE}_engine SETTINGS format_display_secrets_in_show_and_select = 0")"
