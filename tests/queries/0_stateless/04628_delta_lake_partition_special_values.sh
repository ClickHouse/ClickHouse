#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111758
# DeltaLake partitioned INSERT corrupted path-like partition values and rejected NULL:
#   'a/b' was committed and read back as 'a' (the '/' was treated as a path separator and
#   the partition value was reverse-parsed out of the path); '%' produced a file the reader
#   could not open (the committed path was decoded once by unescapeForFileName); NULL threw
#   NOT_IMPLEMENTED. The sink now percent-encodes each value into a single Hive path segment,
#   commits the true value in partitionValues (never parsed from the path), and represents a
#   null (or empty-string) partition value as __HIVE_DEFAULT_PARTITION__ + a JSON null.
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol +
# metaData), because ClickHouse cannot initialize a Delta transaction log itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_part"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# Create an empty partitioned Delta table at $1 with the given JSON schema string ($2) and
# partitionColumns array ($3), using a minimal v0 transaction log (what delta-rs writes for an
# empty overwrite).
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

# List the committed data-file paths (add.path) from the _delta_log, with the random UUID
# file name normalized to a stable token so the reference is deterministic. Only the partition
# directory and its encoding (the thing under test) are shown.
committed_paths() {
    local path="$1"
    grep -h '"add"' "${path}"/_delta_log/*.json \
        | sed -E 's/.*"path":"([^"]*)".*/\1/' \
        | sed -E 's:/[0-9a-f-]{36}\.parquet$:/<uuid>.parquet:' \
        | LC_ALL=C sort
}

# List the committed `partitionValues` object of every add action, sorted. This shows the exact
# JSON committed for a null partition (`{"part":null}`), so a writer that omitted the key (which
# the reader would still materialize as NULL) would be caught here.
committed_partition_values() {
    local path="$1"
    grep -h '"add"' "${path}"/_delta_log/*.json \
        | sed -E 's/.*("partitionValues":\{[^}]*\}).*/\1/' \
        | LC_ALL=C sort
}

# schema for (number Int32 NOT NULL, part Nullable(String)) partitioned by part
SCHEMA_PART='{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"part\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- slash: 'a/b' must round-trip (was truncated to 'a'); committed inside one path segment"
bootstrap "${ROOT}/slash" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/slash') VALUES (1, 'a/b');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/slash') ORDER BY number;
"
echo "committed path: $(committed_paths "${ROOT}/slash")"

echo "-- percent: '%' must round-trip (committed file was previously unreadable)"
bootstrap "${ROOT}/percent" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/percent') VALUES (1, '%');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/percent') ORDER BY number;
"
echo "committed path: $(committed_paths "${ROOT}/percent")"

echo "-- assorted reserved characters must round-trip"
bootstrap "${ROOT}/mixed" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/mixed') VALUES (1, 'a/b'), (2, '%'), (3, 'a b'), (4, 'a=b'), (5, 'plain');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/mixed') ORDER BY number;
"

echo "-- dash and other Hive-safe characters are NOT escaped in the directory (readable names),"
echo "-- but the committed add.path is URI-encoded (space -> %20)"
bootstrap "${ROOT}/dash" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dash') VALUES (1, 'comment-0'), (2, 'a b');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/dash') ORDER BY number;
"
echo "committed paths: $(committed_paths "${ROOT}/dash")"

echo "-- null: NULL partition value must be accepted and read back as NULL (was NOT_IMPLEMENTED)"
bootstrap "${ROOT}/null" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/null') VALUES (1, NULL);
    SELECT number, part FROM deltaLakeLocal('${ROOT}/null') ORDER BY number;
"
echo "committed path: $(committed_paths "${ROOT}/null")"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/null")"

echo "-- empty string is null-equivalent per the Delta protocol (reads back as NULL)"
bootstrap "${ROOT}/empty" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/empty') VALUES (1, '');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/empty') ORDER BY number;
"
echo "committed path: $(committed_paths "${ROOT}/empty")"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/empty")"

echo "-- NULL and the literal string '__HIVE_DEFAULT_PARTITION__' stay distinct"
bootstrap "${ROOT}/collide" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/collide') VALUES (1, NULL), (2, '__HIVE_DEFAULT_PARTITION__');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/collide') ORDER BY number;
"

# schema for (number Int32 NOT NULL, part String NOT NULL) partitioned by part
SCHEMA_NONNULL='{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"part\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}'

echo "-- null-equivalent value into a non-nullable partition column is rejected up front"
bootstrap "${ROOT}/nonnull" "${SCHEMA_NONNULL}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/nonnull') VALUES (1, '');
" 2>&1 | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

echo "-- rejection uses the Delta schema, not the input header: an explicit Nullable structure"
echo "-- over a non-nullable Delta partition column must still reject a NULL"
bootstrap "${ROOT}/nonnull2" "${SCHEMA_NONNULL}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${ROOT}/nonnull2', 'Parquet', 'number Int32, part Nullable(String)') VALUES (1, NULL);
" 2>&1 | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

# schema for (number Int32 NOT NULL, k Nullable(Int32)) partitioned by k
SCHEMA_INT='{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"k\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- non-string (Int32) partition column serializes with toString, not raw bytes"
bootstrap "${ROOT}/int" "${SCHEMA_INT}" '["k"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/int') VALUES (1, 42), (2, -7), (3, NULL);
    SELECT number, k FROM deltaLakeLocal('${ROOT}/int') ORDER BY number;
"

# schema for (number Int32, a Nullable(String), b Nullable(String)) partitioned by a, b
SCHEMA_TWO='{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"a\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- multiple partition columns with reserved characters round-trip"
bootstrap "${ROOT}/two" "${SCHEMA_TWO}" '["a","b"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/two') VALUES (1, 'a/b', 'c%d'), (2, 'p', 'q');
    SELECT number, a, b FROM deltaLakeLocal('${ROOT}/two') ORDER BY number;
"
echo "committed paths: $(committed_paths "${ROOT}/two")"

echo "-- Unicode (UTF-8) partition values round-trip; non-ASCII bytes stay literal in the"
echo "-- directory and are URI-encoded in the committed add.path"
bootstrap "${ROOT}/unicode" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/unicode') VALUES (1, 'café'), (2, '日本語'), (3, 'a/münchen');
    SELECT number, part FROM deltaLakeLocal('${ROOT}/unicode') ORDER BY number;
"
echo "committed paths: $(committed_paths "${ROOT}/unicode")"

echo "-- the legacy (non-kernel) reader must also read a null partition value back as NULL"
echo "-- (the committed JSON null previously threw in the String-only parsing path)"
bootstrap "${ROOT}/legacy_null" "${SCHEMA_PART}" '["part"]'
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/legacy_null') VALUES (1, NULL), (2, 'x'), (3, '');
"
${CLICKHOUSE_LOCAL} --allow_experimental_delta_lake_writes=1 --allow_experimental_delta_kernel_rs=0 --query "
    SELECT number, part FROM deltaLakeLocal('${ROOT}/legacy_null') ORDER BY number;
"
