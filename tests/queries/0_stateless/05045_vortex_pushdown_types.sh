#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The filter pushdown has to give the same answers as the scan without it, over every type and
# predicate shape it translates - and over the ones it must refuse to translate.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        toInt32(number - 500) AS i,
        number AS u,
        toFloat64(number) / 4 AS f,
        toFloat32(number % 8) / 4 AS f32,
        concat('str', toString(number)) AS s,
        (number % 2 = 1)::Bool AS b,
        toDate32('2020-01-01') + number % 200 AS d,
        toDateTime64('2020-01-01 00:00:00', 0, 'UTC') + number AS dt0,
        toDateTime64('2020-01-01 00:00:00.000', 3, 'Asia/Istanbul') + number AS dt3,
        if(number % 3 = 0, NULL, toInt32(number - 500)) AS nb,
        toUInt8(number % 256) AS u8,
        toUInt16(number % 200) AS u16d,
        toLowCardinality(concat('g', toString(number % 7))) AS lc,
        tuple(number)::Tuple(a UInt64) AS t,
        if(number % 5 = 0, nan, toFloat64(number) / 2) AS fn
    FROM numbers(1000)
    FORMAT Vortex" > "$DATA_FILE"

# Each query runs with the pushdown on and off, so every case prints its answer twice.
run_query() {
    local label=$1
    local query=$2
    echo "$label"
    for push_down in 1 0; do
        $CLICKHOUSE_LOCAL -q "$query SETTINGS input_format_vortex_filter_push_down = $push_down"
    done
}

echo "=== Date32 ==="
run_query "d = string literal:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE d = '2020-03-01'"
run_query "d != typed literal:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE d != toDate32('2020-03-01')"
run_query "d < / <= / > / >=:" \
    "SELECT countIf(d < '2020-02-01'), countIf(d <= '2020-02-01'), countIf(d > '2020-06-01'), countIf(d >= '2020-06-01') FROM file('$DATA_FILE', 'Vortex') WHERE d < '2020-02-01' OR d >= '2020-06-01'"
run_query "d range with min/max:" \
    "SELECT count(), min(d), max(d) FROM file('$DATA_FILE', 'Vortex') WHERE d >= '2020-03-01' AND d < '2020-04-01'"

echo "=== DateTime64 ==="
run_query "dt0 (UTC, scale 0) range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE dt0 >= toDateTime64('2020-01-01 00:05:00', 0, 'UTC')"
run_query "dt0 equality:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE dt0 = toDateTime64('2020-01-01 00:07:00', 0, 'UTC')"
run_query "dt3 (Asia/Istanbul, scale 3) range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE dt3 < toDateTime64('2020-01-01 00:01:40.500', 3, 'Asia/Istanbul')"
run_query "dt3 equality on a string literal:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE dt3 = '2020-01-01 00:03:20'"

echo "=== Bool ==="
run_query "b = true:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE b = true"
run_query "b = false:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE b = false"
run_query "b != true:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE b != true"
run_query "bare b:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE b"
run_query "NOT b:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE NOT b"
run_query "Bool header over a U8 file column (not pushed, values above 1 clamp):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'u8 Bool') WHERE u8 = true"

echo "=== Signed and unsigned integers ==="
run_query "negative Int32 bound:" \
    "SELECT count(), min(i), max(i) FROM file('$DATA_FILE', 'Vortex') WHERE i < -490"
run_query "UInt64 against a negative bound (never matches):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE u < -1"
run_query "UInt32 header over a U64 file column (not pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'u UInt32') WHERE u = 5"

echo "=== Nullable ==="
run_query "nb = value:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb = -499"
run_query "nb != value:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb != 0"
run_query "nb IS NULL:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb IS NULL"
run_query "nb IS NOT NULL:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb IS NOT NULL"

echo "=== IN ==="
run_query "IN:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE u IN (1, 5, 999, 12345)"
run_query "NOT IN:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE u NOT IN (1, 5, 999, 12345)"
run_query "IN with a NULL element:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb IN (-499, NULL)"
run_query "NOT IN with a NULL element:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb NOT IN (-499, NULL)"
run_query "IN over the pushdown size cap (not pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE u IN ($(seq -s, 1 65))"
run_query "collapsing tuple NOT IN (must keep every row):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE (u, u) NOT IN ((1, 2))"
run_query "tuple IN (not pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE (u, i) IN ((5, -495))"

echo "=== Strings ==="
run_query "LIKE with a perfect prefix:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE s LIKE 'str99%'"
run_query "LIKE without wildcards (an equality):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE s LIKE 'str7'"
run_query "LIKE with an imperfect prefix:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE s LIKE 'str1%7'"
run_query "NOT LIKE with a perfect prefix:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE s NOT LIKE 'str9%'"
run_query "LIKE by suffix (not pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE s LIKE '%77'"
run_query "startsWith:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE startsWith(s, 'str10')"
run_query "LowCardinality equality:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE lc = 'g3'"

echo "=== Floats ==="
run_query "Float64 range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE f >= 249.5"
run_query "Float32 with a non-representable bound (not pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE f32 > 0.1"
run_query "NaN rows against a range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE fn > 480"
run_query "NaN rows against not-equals:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE fn != 1"

echo "=== Structural ==="
run_query "subcolumn predicate (not pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE t.a = 5"
run_query "predicate on a column missing in the file:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'u UInt64, missing String') WHERE missing = ''"
run_query "OR across types:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE d >= '2020-07-15' OR b = false"
run_query "AND with an untranslatable conjunct:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE d >= '2020-07-01' AND u % 2 = 0"
run_query "NOT over an untranslatable subtree:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE NOT (u % 2 = 0)"
run_query "always-false condition:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE 0"

echo "=== Date over a U16 file column ==="
# ClickHouse writes `Date` as `U16`, so reading it back as `Date` compares the same day numbers.
run_query "Date header, range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'u16d Date') WHERE u16d >= '1970-03-01'"
run_query "Date header, equality on a string literal:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'u16d Date') WHERE u16d = '1970-04-11'"

rm -f "$DATA_FILE"
