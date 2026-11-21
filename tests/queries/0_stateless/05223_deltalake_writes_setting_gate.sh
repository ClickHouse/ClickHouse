#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The `allow_delta_lake_writes` gate on an existing table: compiled default off, rejected INSERTs write
# nothing; the alias, SET and a SETTINGS clause enable writes; read-only sessions are rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_gate"
TABLE="${ROOT}/t"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

mkdir -p "${TABLE}/_delta_log"
cat > "${TABLE}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-gate","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

state() {
    echo "versions: $(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' '), data files: $(find "${TABLE}" -name '*.parquet' | wc -l | tr -d ' ')"
}

echo "-- the compiled-in default is off (test profiles turn it on, so the cases below set it explicitly)"
${CLICKHOUSE_LOCAL} --query "SELECT name, default, tier FROM system.settings WHERE name = 'allow_delta_lake_writes'"

echo "-- setting off: INSERT into the table function is rejected and names the setting"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=0 --query "INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (1)" 2>&1 \
    | grep -o "allow_delta_lake_writes\|SUPPORT_IS_DISABLED" | sort -u
state

echo "-- setting off: INSERT through an engine table is rejected the same way"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=0 --query "
    CREATE TABLE t (id Int32) ENGINE = DeltaLakeLocal('${TABLE}');
    INSERT INTO t VALUES (1);
" 2>&1 | grep -o "SUPPORT_IS_DISABLED"
state

echo "-- the experimental alias enables writes"
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (1);
    SELECT count() FROM deltaLakeLocal('${TABLE}');
"
state

echo "-- SET in the session enables writes"
${CLICKHOUSE_LOCAL} --query "
    SET allow_delta_lake_writes = 1;
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (2);
    SELECT count() FROM deltaLakeLocal('${TABLE}');
"
state

echo "-- a SETTINGS clause on the INSERT statement enables writes for that statement only:"
echo "-- the next INSERT of the same session, without the clause, is rejected again"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=0 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') SETTINGS allow_delta_lake_writes = 1 VALUES (3);
    SELECT count() FROM deltaLakeLocal('${TABLE}');
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (33);
" 2>&1 | grep -oE "^[0-9]+$|SUPPORT_IS_DISABLED" | sort -u
state

echo "-- a read-only session cannot write even with the setting on (enabled before entering readonly)"
${CLICKHOUSE_LOCAL} --query "
    SET allow_delta_lake_writes = 1;
    SET readonly = 1;
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (4);
" 2>&1 | grep -o "READONLY"
state

echo "-- the committed rows are exactly the three accepted inserts"
${CLICKHOUSE_LOCAL} --query "SELECT id FROM deltaLakeLocal('${TABLE}') ORDER BY id"
