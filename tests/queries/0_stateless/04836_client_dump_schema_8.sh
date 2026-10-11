#!/usr/bin/env bash
# Tags: no-fasttest, no-darwin, zookeeper, no-replicated-database
# Tag no-fasttest: S3Queue is not in the fast test build.
# Tag no-replicated-database: S3Queue tables keep their own Keeper state, which the runner's Replicated database wrapper would share.
#
# Server-side carrier: the S3Queue hive-partitioning gate.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
ZK_PREFIX="/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dump_schema_8"
DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
HIVE_RE='^SET allow_experimental_object_storage_queue_hive_partitioning = 1;'
CONSTRAINT_USER="${DB}_queue_user"
CONSTRAINT_PROFILE="${DB}_queue_profile"

cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP DATABASE IF EXISTS ${DB}_plain SYNC;
        DROP DATABASE IF EXISTS ${DB}_hive SYNC;
        DROP DATABASE IF EXISTS ${DB}_constrained SYNC;
        DROP USER IF EXISTS ${CONSTRAINT_USER};
        DROP SETTINGS PROFILE IF EXISTS ${CONSTRAINT_PROFILE};
    " > /dev/null 2>&1 || true
    rm -f "$DUMP_FILE" "$ERR_FILE"
}
trap cleanup EXIT

# ast_fuzzer_any_query = 0: the AST fuzzer would replay the queue DDL as a clone that shares the Keeper path.
# s3_allow_server_credentials_in_user_queries = 1: the queue URL carries no credentials.
QUEUE_URL="http://localhost:11111/test/${DB}/*"

echo '--- the S3Queue hive-partitioning gate follows use_hive_partitioning ---'
$CLICKHOUSE_CLIENT --multiquery --ast_fuzzer_any_query=0 --s3_allow_server_credentials_in_user_queries=1 --query "
    CREATE DATABASE ${DB}_plain;
    CREATE TABLE ${DB}_plain.q (x Int64) ENGINE = S3Queue('${QUEUE_URL}', 'CSV')
        SETTINGS mode = 'unordered', keeper_path = '${ZK_PREFIX}/plain';
"
$CLICKHOUSE_CLIENT --dump-schema="${DB}_plain" > "$DUMP_FILE" 2>"$ERR_FILE"
echo "plain S3Queue, hive gate emitted: $(grep -c "$HIVE_RE" "$DUMP_FILE")"

$CLICKHOUSE_CLIENT --multiquery --ast_fuzzer_any_query=0 --s3_allow_server_credentials_in_user_queries=1 --allow_experimental_object_storage_queue_hive_partitioning=1 --query "
    CREATE DATABASE ${DB}_hive;
    CREATE TABLE ${DB}_hive.q (x Int64) ENGINE = S3Queue('${QUEUE_URL}', 'CSV')
        SETTINGS mode = 'unordered', keeper_path = '${ZK_PREFIX}/hive', use_hive_partitioning = 1;
"
$CLICKHOUSE_CLIENT --dump-schema="${DB}_hive" > "$DUMP_FILE" 2>"$ERR_FILE"
echo "hive S3Queue, hive gate emitted: $(grep -c "$HIVE_RE" "$DUMP_FILE")"

echo '--- a plain S3Queue dump replays under a hive-partitioning constraint ---'
$CLICKHOUSE_CLIENT --multiquery --ast_fuzzer_any_query=0 --s3_allow_server_credentials_in_user_queries=1 --query "
    CREATE DATABASE ${DB}_constrained;
    CREATE TABLE ${DB}_constrained.q (x Int64) ENGINE = S3Queue('${QUEUE_URL}', 'CSV')
        SETTINGS mode = 'unordered', keeper_path = '${ZK_PREFIX}/constrained';
"
$CLICKHOUSE_CLIENT --dump-schema="${DB}_constrained" > "$DUMP_FILE" 2>"$ERR_FILE"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE ${DB}_constrained SYNC;
    DROP USER IF EXISTS ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE IF EXISTS ${CONSTRAINT_PROFILE};
    CREATE SETTINGS PROFILE ${CONSTRAINT_PROFILE} SETTINGS allow_experimental_object_storage_queue_hive_partitioning = 0 CONST;
    CREATE USER ${CONSTRAINT_USER} SETTINGS PROFILE '${CONSTRAINT_PROFILE}';
    GRANT ALL ON *.* TO ${CONSTRAINT_USER};
"
$CLICKHOUSE_CLIENT --user "$CONSTRAINT_USER" --ast_fuzzer_any_query=0 --s3_allow_server_credentials_in_user_queries=1 --multiquery --queries-file "$DUMP_FILE" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: constrained replay succeeded' || echo "FAIL: constrained replay rejected: $(cat "$ERR_FILE")"
echo "constrained replay table present: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${DB}_constrained' AND name = 'q'")"
