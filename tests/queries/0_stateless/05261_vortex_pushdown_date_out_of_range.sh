#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading a `vortex.date` day number that does not fit into a `Date` header throws
# `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE` (unless `date_time_overflow_behavior = 'saturate'`). A
# pushed predicate on that column must not filter such a row out of the scan before it is decoded,
# or the same query would throw without the pushdown and succeed with it.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

# One row before 1970-01-01, which `Date32` can hold and `Date` cannot.
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, if(number = 7, toDate32('1960-01-01'), toDate32('2020-01-01') + number) AS d
    FROM numbers(100)
    FORMAT Vortex" > "$DATA_FILE"

# Every case runs in its own process, so the profile events belong to its query alone.
run() {
    local label=$1
    local query=$2
    echo "$label"
    $CLICKHOUSE_LOCAL --ignore-error -q "
        $query;
        SELECT
            ifNull((SELECT value FROM system.events WHERE event = 'VortexFilterPushdownConjunctsPushed'), 0),
            ifNull((SELECT value FROM system.events WHERE event = 'VortexFilterPushdownConjunctsDropped'), 0)" 2>&1 \
        | grep -o -E 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE|^[0-9]+(\s[0-9]+)?$'
}

for predicate in "d >= '2020-01-01'" "d = '2020-01-10'" "d IN ('2020-01-10', '2020-01-20')" "d NOT IN ('2020-01-10')"; do
    run "Date header, $predicate (pushed, still throws):" \
        "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date') WHERE $predicate"
done

run "Date header without the pushdown (throws):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date') WHERE d >= '2020-01-01' SETTINGS input_format_vortex_filter_push_down = 0"

run "Date32 header (pushed, the day fits):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date32') WHERE d >= '2020-01-01'"

run "Date header under date_time_overflow_behavior = 'saturate' (dropped, the day is clamped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date') WHERE d >= '2020-01-01' SETTINGS date_time_overflow_behavior = 'saturate'"

rm -f "$DATA_FILE"
