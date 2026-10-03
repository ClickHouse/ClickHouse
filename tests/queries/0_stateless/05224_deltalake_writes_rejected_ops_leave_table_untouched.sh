#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Non-append statements and rejected INSERTs must fail closed: user error, no new version, no data
# file, committed rows intact. All statements run in one clickhouse-local process
# (`--ignore-error`, error codes taken from the server log) to keep the test fast under sanitizers.

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
    echo "versions: $(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' '), data files: $(find "${path}" -name '*.parquet' | wc -l | tr -d ' ')"
}

# (id integer NOT NULL, s string nullable)
SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
TABLE="${ROOT}/t"
bootstrap "${TABLE}" "${SCHEMA}" '[]'
# (a integer, b string) partitioned by both columns: there is no data column left for the file.
SCHEMA_ALL_PART='{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
ALL_PART="${ROOT}/all_part"
bootstrap "${ALL_PART}" "${SCHEMA_ALL_PART}" '["a","b"]'

${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') VALUES (1, 'a'), (2, 'b'), (3, 'c');
"
echo "-- baseline after one committed append"
state "${TABLE}"

# Every statement is announced by a marker row; the error code of the statement that follows is
# taken from the server log line, so the output reads as "statement, then its error code".
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --ignore-error --send_logs_level=error --query "
    CREATE TABLE t (id Int32, s String) ENGINE = DeltaLakeLocal('${TABLE}');
    SELECT '-- DELETE FROM t WHERE id = 1';
    DELETE FROM t WHERE id = 1;
    SELECT '-- ALTER TABLE t DELETE WHERE id = 1';
    ALTER TABLE t DELETE WHERE id = 1;
    SELECT '-- ALTER TABLE t UPDATE s = ''z'' WHERE 1';
    ALTER TABLE t UPDATE s = 'z' WHERE 1;
    SELECT '-- TRUNCATE TABLE t';
    TRUNCATE TABLE t;
    SELECT '-- OPTIMIZE TABLE t';
    OPTIMIZE TABLE t;
    SELECT '-- OPTIMIZE TABLE t FINAL';
    OPTIMIZE TABLE t FINAL;
    SELECT '-- ALTER TABLE t ADD COLUMN extra Int32';
    ALTER TABLE t ADD COLUMN extra Int32;
    SELECT '-- ALTER TABLE t DROP COLUMN s';
    ALTER TABLE t DROP COLUMN s;
    SELECT '-- ALTER TABLE t MODIFY COLUMN s Int32';
    ALTER TABLE t MODIFY COLUMN s Int32;
    SELECT '-- ALTER TABLE t RENAME COLUMN s TO renamed';
    ALTER TABLE t RENAME COLUMN s TO renamed;
    SELECT '-- INSERT with a column the table does not have';
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE}', 'Parquet', 'id Int32, s String, extra Int32') VALUES (4, 'd', 1);
    SELECT '-- INSERT of NULL into a non-nullable data column';
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE}', 'Parquet', 'id Nullable(Int32), s String') VALUES (NULL, 'd');
    SELECT '-- INSERT that throws while streaming rows';
    INSERT INTO FUNCTION deltaLakeLocal('${TABLE}') SELECT (throwIf(number = 5, 'boom') + number)::Int32 AS id, 'x' AS s FROM numbers(10) SETTINGS max_block_size = 1;
    SELECT '-- INSERT into a table whose every column is a partition column';
    INSERT INTO FUNCTION deltaLakeLocal('${ALL_PART}') VALUES (1, 'x');
    SELECT '-- the original rows are still readable after all rejected statements';
    SELECT id, s FROM t ORDER BY id;
" 2>&1 < /dev/null | grep -oE "^-- .*|^[0-9]+	[a-z]$|executeQuery: Code: [0-9]+\. .*\([A-Z_]+\)" | sed -E 's/^executeQuery: Code: [0-9]+\. .*\(([A-Z_]+)\).*/\1/'

echo "-- nothing was written by any rejected statement"
state "${TABLE}"
state "${ALL_PART}"
