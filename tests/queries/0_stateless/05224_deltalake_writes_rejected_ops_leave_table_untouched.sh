#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Delta Lake tables support append only. Every other mutating statement, and every INSERT that is
# rejected before its commit, must fail closed: a user-facing error (never a logical error), no new
# `_delta_log` version, no data file left behind, and the previously committed rows intact.
# A statement that "succeeds" as a silent no-op is a bug too (OPTIMIZE used to do that).
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol + metaData).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_fail_closed"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

bootstrap() {
    local path="$1"
    local schema="$2"
    local partition_cols="$3"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"${schema}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

state() {
    local path="$1"
    echo "versions: $(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' '), data files: $(find "${path}" -name '*.parquet' | wc -l | tr -d ' '), rows: $(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM deltaLakeLocal('${path}')")"
}

# Print only the error code name of a failing statement, so the reference stays stable across
# message wording changes while still catching a LOGICAL_ERROR or an unexpected success.
error_code() {
    grep -oE '\([A-Z_]+\)' | head -1
}

# (id integer NOT NULL, s string nullable)
SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
TABLE="${ROOT}/t"
bootstrap "${TABLE}" "${SCHEMA}" '[]'

${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (1, 'a'), (2, 'b'), (3, 'c');
"
echo "-- baseline after one committed append"
state "${TABLE}"

for statement in \
    "DELETE FROM t WHERE id = 1" \
    "ALTER TABLE t DELETE WHERE id = 1" \
    "ALTER TABLE t UPDATE s = 'z' WHERE 1" \
    "TRUNCATE TABLE t" \
    "OPTIMIZE TABLE t" \
    "OPTIMIZE TABLE t FINAL" \
    "ALTER TABLE t ADD COLUMN extra Int32" \
    "ALTER TABLE t DROP COLUMN s" \
    "ALTER TABLE t MODIFY COLUMN s Int32" \
    "ALTER TABLE t RENAME COLUMN s TO renamed"
do
    echo "-- ${statement}"
    ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
        CREATE TABLE t (id Int32, s String) ENGINE = DeltaLakeLocal('${TABLE}');
        ${statement};
    " 2>&1 | error_code
    state "${TABLE}"
done

echo "-- INSERT with a column the table does not have is rejected before any file is written"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE}', 'Parquet', 'id Int32, s String, extra Int32') VALUES (4, 'd', 1);
" 2>&1 | error_code
state "${TABLE}"

echo "-- INSERT of NULL into a non-nullable data column is rejected before any file is written"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE}', 'Parquet', 'id Nullable(Int32), s String') VALUES (NULL, 'd');
" 2>&1 | error_code
state "${TABLE}"

echo "-- INSERT that throws while streaming rows leaves nothing behind"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') SELECT (throwIf(number = 5, 'boom') + number)::Int32 AS id, 'x' AS s FROM numbers(10) SETTINGS max_block_size = 1;
" 2>&1 | error_code
state "${TABLE}"

# (a integer, b string) partitioned by both columns: there is no data column left for the file.
SCHEMA_ALL_PART='{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
ALL_PART="${ROOT}/all_part"
bootstrap "${ALL_PART}" "${SCHEMA_ALL_PART}" '["a","b"]'

echo "-- every column is a partition column: rejected as a user error, nothing written"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ALL_PART}') VALUES (1, 'x');
" 2>&1 | error_code
state "${ALL_PART}"

echo "-- the original rows are still readable after all rejected statements"
${CLICKHOUSE_LOCAL} --query "SELECT id, s FROM deltaLakeLocal('${TABLE}') ORDER BY id"
