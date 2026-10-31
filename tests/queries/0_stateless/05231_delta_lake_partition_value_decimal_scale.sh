#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/120521
# A DeltaLake decimal partition value did not survive a write-then-read, for two reasons.
#
# Write: the partition value was committed with fewer fractional digits than the column
# scale (1.50 as "1.5", 10.00 as "10", with no decimal point at all), because it was rendered
# with toString, whose Decimal path drops trailing fractional zeros. delta-kernel-rs requires
# the fractional digit count to equal the declared scale exactly, so SELECT on the table
# ClickHouse had just written failed with DELTA_KERNEL_ERROR, and delta-rs could not read it
# either. Spark writes the scale-exact form and tolerates both.
#
# Read: a decimal partition value with precision 19 or higher (Decimal128) had its two 64-bit
# halves transposed while being decoded, and came back as a different number with no error.
# That half is engine-independent: it also affects Spark-written and delta-rs-written tables.
#
# The same serializer got a timestamp partition value wrong in the same way: Delta commits it as a
# UTC wall clock, so rendering it in the session time zone shifted the value by the zone offset with
# no error, and the two Delta timestamp types do not share one form. The cases at the end cover
# that, plus every other Delta primitive type as a partition column, since which types need their
# own text conversion is what the writer decides.
#
# Every case asserts BOTH the exact committed partitionValues JSON (the protocol string under
# test) and a SELECT returning the value: the JSON alone would not prove readability, and the
# SELECT alone would not distinguish the scale-exact form from a lenient parse.
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
# empty overwrite). $4 overrides the protocol action for a table that needs a reader/writer
# feature.
bootstrap() {
    local path="$1"
    local schema="$2"
    local partition_cols="$3"
    local protocol="${4:-{\"minReaderVersion\":1,\"minWriterVersion\":2\}}"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":${protocol}}
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

# A (id Int32 NOT NULL, p Nullable(<delta type>)) schema partitioned by p.
schema_for() {
    printf '{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"integer\\",\\"nullable\\":false,\\"metadata\\":{}},{\\"name\\":\\"p\\",\\"type\\":\\"%s\\",\\"nullable\\":true,\\"metadata\\":{}}]}' "$1"
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

echo "-- decimal(38,10) (Decimal128 width): the fraction is padded to the full scale, and the"
echo "-- value must read back unchanged. The unscaled magnitude of the third value exceeds 2^64,"
echo "-- so it is the row that pins the high half of the decode; the negative one pins both signs."
bootstrap "${ROOT}/dec38" "${SCHEMA_DEC38}" '["p"]'
# Explicit toDecimal128 expressions: a bare 29-digit literal is parsed as Float64 and would
# lose precision before the sink ever sees it.
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dec38') VALUES
        (1, toDecimal128('1.5', 10)),
        (2, toDecimal128('-1.5', 10)),
        (3, toDecimal128('1234567890123456789.0123456789', 10));
    SELECT id, p FROM deltaLakeLocal('${ROOT}/dec38') ORDER BY id;
"
echo "committed partitionValues:"
committed_partition_values "${ROOT}/dec38"

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

# (id Int32 NOT NULL, p Nullable(Decimal(9, 2)), s Nullable(String)) partitioned by p, s
SCHEMA_TWO='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"decimal(9,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- a decimal partition column composes with a non-decimal one (which is unaffected);"
echo "-- decimal(9,2) is also the Decimal32 decode arm, which must stay correct"
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

# (id Int32 NOT NULL, p Nullable(Decimal(19, 0))) partitioned by p
SCHEMA_DEC19='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"decimal(19,0)\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- precision 19 is the first width that maps to Decimal128, and at scale 0 the committed"
echo "-- string carries no fraction, so this table is byte-identical to what Spark writes and to"
echo "-- what ClickHouse wrote before the write fix: only the decode can get it wrong"
bootstrap "${ROOT}/dec19" "${SCHEMA_DEC19}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/dec19') VALUES (1, 5);
    SELECT id, p FROM deltaLakeLocal('${ROOT}/dec19') ORDER BY id;
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/dec19")"

# Write $3 (a VALUES list) into a fresh table partitioned by a decimal($1, $2) column, then show
# what was committed and what comes back.
decimal_case() {
    local precision="$1"
    local scale="$2"
    local values="$3"
    local dir="${ROOT}/dec_${precision}_${scale}"
    echo "-- decimal(${precision},${scale})"
    bootstrap "${dir}" "$(schema_for "decimal(${precision},${scale})")" '["p"]'
    ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
        INSERT INTO FUNCTION deltaLakeLocal('${dir}') VALUES ${values};
        SELECT id, p FROM deltaLakeLocal('${dir}') ORDER BY id;
    "
    echo "committed partitionValues:"
    committed_partition_values "${dir}"
}

echo "-- the boundary shapes of the Delta decimal range: scale equal to the precision (no integer"
echo "-- digit at all), the maximum precision with no fraction (a magnitude that needs the full"
echo "-- Decimal128 width, so it also pins the decode of both 64-bit halves), and the two"
echo "-- narrowest widths. Each committed value must still carry exactly <scale> fractional digits."
decimal_case 38 38 "(1, toDecimal128('0.5', 38)), (2, toDecimal128('-0.5', 38)), (3, toDecimal128('0.12345678901234567890123456789012345678', 38))"
decimal_case 38 0 "(1, toDecimal128('99999999999999999999999999999999999999', 0)), (2, toDecimal128('-99999999999999999999999999999999999999', 0))"
decimal_case 1 0 "(1, toDecimal32('9', 0)), (2, toDecimal32('-9', 0))"
decimal_case 1 1 "(1, toDecimal32('0.9', 1)), (2, toDecimal32('-0.9', 1)), (3, toDecimal32('0', 1))"

# (id Int32 NOT NULL, p Nullable(DateTime64(6)), d Nullable(DateTime64(6))) partitioned by p
SCHEMA_TS='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"d\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- a Delta timestamp is adjusted to UTC, and for it the protocol asks writers for the ISO8601"
echo "-- Z form, which is also what delta-kernel-rs itself commits (format_timestamp). The write"
echo "-- schema type is DateTime64(6) with no explicit time zone, so a plain toString renders the"
echo "-- session zone in the space-separated form, which the protocol defines as the writer's local"
echo "-- time. p (partitioned) and d (a plain column) are given the same instant under a non-UTC"
echo "-- session zone: before the fix p was committed as the Tokyo wall clock and came back 9 hours"
echo "-- off d, with no error."
bootstrap "${ROOT}/ts" "${SCHEMA_TS}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --session_timezone='Asia/Tokyo' --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/ts')
        SELECT 1 AS id,
               toDateTime64('2026-09-18 12:34:56.123456', 6) AS p,
               toDateTime64('2026-09-18 12:34:56.123456', 6) AS d;
    SELECT toString(p, 'UTC') AS p_utc, toString(d, 'UTC') AS d_utc, p = d AS partition_matches_data
    FROM deltaLakeLocal('${ROOT}/ts');
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/ts")"
echo "committed paths: $(committed_paths "${ROOT}/ts")"

# The same schema with timestamp_ntz, which the kernel only accepts when the table declares the
# feature. This type has no time zone, so the ISO Z form above is not merely non-canonical for it
# but unparseable (the kernel retries that pattern only for timestamp), and it keeps the
# space-separated form of the kernel's own writer (format_timestamp_ntz: no Z, naive_utc). This is
# the case that makes the write schema insufficient: it collapses both Delta types to DateTime64(6).
SCHEMA_TS_NTZ='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}},{\"name\":\"d\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- timestamp_ntz has no time zone, so it keeps the space-separated form, and it was shifted"
echo "-- the same way before the fix"
bootstrap "${ROOT}/ts_ntz" "${SCHEMA_TS_NTZ}" '["p"]' \
    '{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --session_timezone='Asia/Tokyo' --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/ts_ntz')
        SELECT 1 AS id,
               toDateTime64('2026-09-18 12:34:56.123456', 6) AS p,
               toDateTime64('2026-09-18 12:34:56.123456', 6) AS d;
    SELECT toString(p, 'UTC') AS p_utc, toString(d, 'UTC') AS d_utc, p = d AS partition_matches_data
    FROM deltaLakeLocal('${ROOT}/ts_ntz');
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/ts_ntz")"

# (id Int32 NOT NULL, one partition column per remaining Delta primitive type, each paired with a
# plain column of the same type holding the same value) partitioned by the six partition columns.
SCHEMA_TYPES='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"pb\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"db\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pd\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dd\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pl\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dl\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pf\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"df\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ps\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ds\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pn\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dn\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- the remaining Delta primitive types need no per-type conversion under default settings, and"
echo "-- this pins that: each is written both as a partition column and as a plain column, and the"
echo "-- two must agree."
bootstrap "${ROOT}/types" "${SCHEMA_TYPES}" '["pb","pd","pl","pf","ps","pn"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/types')
        SELECT 1 AS id,
               true AS pb, true AS db,
               toDate32('2026-09-18') AS pd, toDate32('2026-09-18') AS dd,
               -9223372036854775808 AS pl, -9223372036854775808 AS dl,
               0.1 AS pf, 0.1 AS df,
               'a b' AS ps, 'a b' AS ds,
               'ab' AS pn, 'ab' AS dn;
    SELECT pb = db, pd = dd, pl = dl, pf = df, ps = ds, pn = dn FROM deltaLakeLocal('${ROOT}/types');
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/types")"

# A Delta boolean partition value is the literal true/false the kernel accepts, but Bool's text form
# follows bool_true_representation/bool_false_representation, so a session that sets those committed
# a string the kernel cannot parse. The values read back below are rendered with the same non-default
# settings, so a run where they failed to apply would not pass either. The NULL row is here because
# the setting-independent form still has to leave a null partition value null.
SCHEMA_BOOL='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"d\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- a boolean partition value does not follow bool_true_representation: before the fix a session"
echo "-- using yes/no committed yes, and the table then failed to read at all with a kernel ParseError"
bootstrap "${ROOT}/bool" "${SCHEMA_BOOL}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --bool_true_representation='yes' --bool_false_representation='no' --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/bool')
        SELECT 1 AS id, true AS p, true AS d
        UNION ALL SELECT 2 AS id, false AS p, false AS d
        UNION ALL SELECT 3 AS id, NULL AS p, NULL AS d;
    SELECT id, p, d, p = d AS partition_matches_data, p IS NULL AS is_null
    FROM deltaLakeLocal('${ROOT}/bool') ORDER BY id;
"
echo "committed partitionValues: $(committed_partition_values "${ROOT}/bool")"
echo "committed paths: $(committed_paths "${ROOT}/bool")"
