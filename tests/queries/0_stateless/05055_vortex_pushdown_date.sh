#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ClickHouse writes `Date` as Arrow `DATE32`, so a `Date` header over a file ClickHouse itself
# wrote meets the `vortex.date` carrier - not the `U16` one that `05045_vortex_pushdown_types`
# covers. The day numbers are the same on both sides, so the predicate has to be pushed; that
# cannot be seen from the results, because ClickHouse reapplies the `WHERE` either way.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

# The late dates sit in exactly one of the splits (they hold at most 100 000 rows each), so a
# pushed predicate on them provably drops whole splits.
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, toDate('2020-01-01') + intDiv(number, 100000) * 100 AS d
    FROM numbers(300000)
    FORMAT Vortex" > "$DATA_FILE"

echo "The column is written as vortex.date, so inference gives Date32:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_FILE', 'Vortex')" | cut -f1,2

# Every case runs in its own process, so the profile events belong to its query alone.
run_and_report_events() {
    local label=$1
    local query=$2
    echo "$label"
    $CLICKHOUSE_LOCAL -q "
        $query;
        SELECT
            ifNull((SELECT value FROM system.events WHERE event = 'VortexFilterPushdownConjunctsPushed'), 0),
            ifNull((SELECT value FROM system.events WHERE event = 'VortexFilterPushdownConjunctsDropped'), 0),
            (SELECT count() FROM system.events WHERE event = 'VortexScanEmptySplits' AND value >= 1)"
}

run_and_report_events "Date header, range (pushed, whole splits are dropped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date') WHERE d >= '2020-07-01'"

run_and_report_events "Date header, equality on a string literal (pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date') WHERE d = '2020-04-10'"

run_and_report_events "Date32 header over the same file column (pushed):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date32') WHERE d >= '2020-07-01'"

# Under `Saturate` an out-of-range day is clamped onto a bound, so an equality on that bound would
# match rows it must not - the conjunct has to be dropped instead of pushed.
run_and_report_events "Date header under date_time_overflow_behavior = 'saturate' (dropped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date') WHERE d >= '2020-07-01' SETTINGS date_time_overflow_behavior = 'saturate'"

echo "The pushdown does not change the answers:"
for push_down in 1 0; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), min(d), max(d)
        FROM file('$DATA_FILE', 'Vortex', 'n UInt64, d Date')
        WHERE d >= '2020-04-01' AND d < '2020-07-01'
        SETTINGS input_format_vortex_filter_push_down = $push_down"
done

rm -f "$DATA_FILE"
