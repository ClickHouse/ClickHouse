#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/120521
# A partitioned DeltaLake INSERT committed a decimal partition value with fewer fractional
# digits than the column scale: 1.50 was committed as "1.5" and 10.00 as "10" (no decimal
# point at all), because the value was rendered with toString, whose Decimal path drops
# trailing fractional zeros. delta-kernel-rs requires the fractional digit count to equal the
# declared scale exactly, so SELECT on the table ClickHouse had just written failed with
# DELTA_KERNEL_ERROR, and delta-rs and Spark could not read it either.
#
# Every case asserts BOTH the exact committed partitionValues JSON (the protocol string under
# test) and a successful SELECT: the JSON alone would not prove readability, and the SELECT
# alone would not distinguish the scale-exact form from a lenient parse.
#
# The empty Delta tables are bootstrapped by hand (a v0 _delta_log with only protocol +
# metaData), because ClickHouse cannot initialize a Delta transaction log itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_dec"
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
# JSON committed for a null partition (`{"p":null}`), so a writer that omitted the key (which
# the reader would still materialize as NULL) would be caught here.
committed_partition_values() {
    local path="$1"
    grep -h '"add"' "${path}"/_delta_log/*.json \
        | sed -E 's/.*("partitionValues":\{[^}]*\}).*/\1/' \
        | LC_ALL=C sort
}

# (id Int32 NOT NULL, p Nullable(Decimal(10, 2))) partitioned by p
SCHEMA_DEC2='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- decimal(10,2): every committed value must carry exactly 2 fractional digits."
echo "-- Before the fix 1.50 was committed as \"1.5\", 10.00 as \"10\", -1.50 as \"-1.5\" and"
echo "-- -0.50 as \"-0.5\". -0.05 and 1.05 are the controls that already had a full-width"
echo "-- fraction, so they must be committed unchanged."
bootstrap "${ROOT}/dec2" "${SCHEMA_DEC2}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dec2') VALUES (1, 1.5), (2, 10), (3, -1.5), (4, -0.05), (5, 1.05), (6, -0.5);
    SELECT id, p FROM deltaLakeLocal('${ROOT}/dec2') ORDER BY id;
"
echo "committed partitionValues:"
committed_partition_values "${ROOT}/dec2"
echo "committed paths:"
committed_paths "${ROOT}/dec2"

# (id Int32 NOT NULL, p Nullable(Decimal(38, 10))) partitioned by p
SCHEMA_DEC38='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"decimal(38,10)\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- decimal(38,10) (Decimal128 width): the fraction is padded to the full scale"
bootstrap "${ROOT}/dec38" "${SCHEMA_DEC38}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dec38') VALUES (1, 1.5);
    SELECT id, p FROM deltaLakeLocal('${ROOT}/dec38') ORDER BY id;
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/dec38")"

# (id Int32 NOT NULL, p Nullable(Decimal(10, 0))) partitioned by p
SCHEMA_DEC0='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"decimal(10,0)\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- decimal(10,0) already conformed and must stay unchanged: no decimal point is added"
bootstrap "${ROOT}/dec0" "${SCHEMA_DEC0}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dec0') VALUES (1, 5);
    SELECT id, p FROM deltaLakeLocal('${ROOT}/dec0') ORDER BY id;
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/dec0")"
echo "committed paths: $(committed_paths "${ROOT}/dec0")"

echo "-- a NULL decimal partition value still uses the placeholder directory and a JSON null,"
echo "-- alongside a scale-exact non-null value in the same table"
bootstrap "${ROOT}/dec_null" "${SCHEMA_DEC2}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dec_null') VALUES (1, NULL), (2, 1.5);
    SELECT id, p FROM deltaLakeLocal('${ROOT}/dec_null') ORDER BY id;
"
echo "committed partitionValues:"
committed_partition_values "${ROOT}/dec_null"
echo "committed paths:"
committed_paths "${ROOT}/dec_null"

# (id Int32 NOT NULL, p Nullable(Decimal(10, 2)), s Nullable(String)) partitioned by p, s
SCHEMA_TWO='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- a decimal partition column composes with a non-decimal one (which is unaffected)"
bootstrap "${ROOT}/two" "${SCHEMA_TWO}" '["p","s"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/two') VALUES (1, 1.5, 'x');
    SELECT id, p, s FROM deltaLakeLocal('${ROOT}/two') ORDER BY id;
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/two")"
echo "committed paths: $(committed_paths "${ROOT}/two")"

echo "-- the plain (non-accurate) write cast produces the same protocol form"
bootstrap "${ROOT}/plain_cast" "${SCHEMA_DEC2}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --delta_lake_accurate_write_cast=0 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/plain_cast') VALUES (1, 1.5);
    SELECT id, p FROM deltaLakeLocal('${ROOT}/plain_cast') ORDER BY id;
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/plain_cast")"
