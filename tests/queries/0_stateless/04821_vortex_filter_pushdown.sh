#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

# Runs the query twice: with the filter pushdown enabled and disabled. The two results must be
# identical, so every case below prints its result twice.
run_query() {
    local label=$1
    local query=$2
    echo "$label"
    for push_down in 1 0; do
        $CLICKHOUSE_LOCAL -q "$query SETTINGS input_format_vortex_filter_push_down = $push_down"
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

rm -f "$DATA_FILE"
