#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pushdown has to give the same answers as the scan without it, and has to actually reach the
# scan - which result equivalence alone cannot show, since ClickHouse reapplies WHERE either way.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        number AS n,
        toString(number) AS s,
        if(number % 2 = 0, NULL, number % 100) AS nb,
        toLowCardinality(toString(number % 5)) AS lc,
        toFloat64(number) / 4 AS f,
        toFloat32(number % 8) / 4 AS f32
    FROM numbers(10000)
    FORMAT Vortex" > "$DATA_FILE"

# A wide payload column nobody selects: if the predicate reaches the scan, its bytes stay unread.
PUSHDOWN_DATA_FILE=$CUR_DIR/pushdown_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex
$CLICKHOUSE_LOCAL -q "
    SELECT
        number AS n,
        toString(number) AS s,
        toFloat64(number) / 4 AS f,
        concat(toString(number), repeat('x', 512)) AS payload
    FROM numbers(50000)
    FORMAT Vortex" > "$PUSHDOWN_DATA_FILE"

get_read_bytes() {
    local push_down=$1
    local predicate=$2
    $CLICKHOUSE_LOCAL -q "
        SELECT sum(length(payload))
        FROM file('$PUSHDOWN_DATA_FILE', 'Vortex')
        WHERE $predicate
        SETTINGS input_format_vortex_filter_push_down = $push_down
        FORMAT JSON" | jq -r '.statistics.bytes_read'
}

assert_scan_pushdown() {
    local label=$1
    local predicate=$2
    local read_bytes_with_pushdown
    local read_bytes_without_pushdown
    read_bytes_with_pushdown=$(get_read_bytes 1 "$predicate")
    read_bytes_without_pushdown=$(get_read_bytes 0 "$predicate")
    if [ "$read_bytes_with_pushdown" -lt "$read_bytes_without_pushdown" ]; then
        echo "$label: ok"
    else
        echo "$label did not reduce scan bytes: on=$read_bytes_with_pushdown off=$read_bytes_without_pushdown"
        exit 1
    fi
}

assert_scan_pushdown "Integer pushdown reduces scan bytes" "n = 1"
assert_scan_pushdown "String pushdown reduces scan bytes" "s = '1'"
assert_scan_pushdown "Float pushdown reduces scan bytes" "f = 0.25"

# Same query twice, pushdown on and off, so every case below prints its answer twice.
# All of them run in one `clickhouse-local`; the labels go through it as well, or they would not
# keep their place among the answers.
QUERIES=""

run_query() {
    local label=$1
    local query=$2
    QUERIES+="SELECT '$label';"
    for push_down in 1 0; do
        QUERIES+="$query SETTINGS input_format_vortex_filter_push_down = $push_down;"
    done
}

run_query "Point predicate:" \
    "SELECT n, s FROM file('$DATA_FILE', 'Vortex') WHERE n = 5"
run_query "Range predicate:" \
    "SELECT count(), min(n), max(n) FROM file('$DATA_FILE', 'Vortex') WHERE n > 9990"
run_query "Bounded range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n >= 17 AND n < 21"
run_query "String equality:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE s = '123'"
run_query "IN set:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n IN (1, 5, 9999, 12345)"
run_query "IS NULL:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb IS NULL"
run_query "Equality on a nullable column:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb = 3"
run_query "Inequality on a nullable column:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE nb != 3"
run_query "NOT:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE NOT (n < 9995)"
run_query "NOT over AND with an untranslatable conjunct:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE NOT (n < 9990 AND n % 2 = 0)"
run_query "OR across columns:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n < 3 OR s = '9999'"
run_query "Float64 range:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE f >= 2499.5"
run_query "Float32 with an exactly representable bound:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE f32 > 1.5"
run_query "Float32 with a non-representable bound (not pushed down):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE f32 > 0.1"
run_query "LowCardinality column:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE lc = '3'"
run_query "AND of two pushed-down predicates:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n > 100 AND lc = '0'"
run_query "Predicate on a column missing in the file:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, missing String') WHERE missing = ''"
run_query "Predicate on a column read with a different type:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt32') WHERE n = 5"
run_query "LowCardinality target type:" \
    "SELECT toTypeName(lc), lc FROM file('$DATA_FILE', 'Vortex', 'lc LowCardinality(String)') WHERE lc = '4' LIMIT 1"

$CLICKHOUSE_LOCAL -q "$QUERIES"

rm -f "$DATA_FILE" "$PUSHDOWN_DATA_FILE"
