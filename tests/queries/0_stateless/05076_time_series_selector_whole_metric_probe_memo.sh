#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# `timeSeriesSelector` gates its whole-metric primary-key range on a probe query over the tags
# table, and the probe's counterexample-found verdict is reused for a bounded number of later
# queries of the same shape. Two properties of that reuse are checked here.
#
# A. The opposite verdict is never reused. A selector that matches the whole metric today stops
#    matching it once a series is written with an id outside the metric's range; reusing the old
#    verdict would keep emitting the range, which filters the out-of-range series out of the result.
# B. A reused verdict is retired after a bounded number of uses, so a selector that becomes
#    whole-metric again is probed again. The reuse key excludes the query's time bounds, so a
#    narrowed window - whose own probe finds no counterexample - reuses the entry until it expires;
#    that is what this scenario uses to make the retirement observable.
#
# The range conditions carry the max-UUID literal, which is how the emission is detected below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 --session_timezone UTC"

# Prints 1 if the built plan carries the whole-metric id range, 0 otherwise. Each call builds the
# plan once, i.e. consumes exactly one probe verdict.
has_id_range_query() {
    echo "SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%'
          FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
                FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector($1, '$2', $3, $4)));"
}

has_id_range() {
    $CH -q "$(has_id_range_query "$1" "$2" "$3" "$4")"
}

selector_rows() {
    $CH -q "SELECT timestamp, value FROM timeSeriesSelector($1, '$2', $3, $4) ORDER BY value, timestamp;"
}

echo "-- A. the whole-metric verdict is not reused after an out-of-range series appears"

$CH -q "
DROP TABLE IF EXISTS ts_a SYNC;
CREATE TABLE ts_a ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
INSERT INTO ts_a (metric_name, tags, time_series) VALUES ('foo', map('env', 'a'), [(toDateTime64(100, 3), 1.)]);
"

# The selector matches the whole metric, so the range is emitted. This verdict must not be reused.
echo "range emitted while the selector matches the whole metric: $(has_id_range ts_a foo 0 1000)"

# The setting overrides the tags-table `id` column DEFAULT, so this series gets an id whose first
# component is not the metric's; resetting it restores the canonical generator, which is what the
# probe requires before it runs at all.
$CH -q "
ALTER TABLE ts_a MODIFY SETTING id_generator = 'tuple(sipHash64(tags), reinterpretAsUUID(sipHash128(metric_name, tags)))';
INSERT INTO ts_a (metric_name, tags, time_series) VALUES ('foo', map('env', 'b'), [(toDateTime64(200, 3), 2.)]);
ALTER TABLE ts_a RESET SETTING id_generator;
"

echo "the two series have distinct first id components: $($CH -q "
    SELECT uniqExact(tupleElement(id, 1)) = 2 FROM \`.inner_id.tags.$($CH -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'ts_a'")\`;")"

echo "range emitted after the out-of-range series appeared: $(has_id_range ts_a foo 0 1000)"
echo "both series are returned:"
selector_rows ts_a foo 0 1000

echo "-- B. a reused verdict is retired after a bounded number of uses"

$CH -q "
DROP TABLE IF EXISTS ts_b SYNC;
CREATE TABLE ts_b ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
INSERT INTO ts_b (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]),
    ('foo', map('env', 'stag'), [(toDateTime64(900, 3), 99.)]);
"

NARROW_SELECTOR='foo{env=~"prod|dev"}'

# Sensitivity control: in the narrow window the series that fails the matcher is not time-eligible,
# so this selector's own probe finds no counterexample and the range IS emitted. Without this the
# rest of the scenario could not tell reuse from a genuinely non-whole-metric selector.
echo "narrow window is whole-metric on its own probe: $(has_id_range ts_b "$NARROW_SELECTOR" 0 200)"

ROWS_FRESH=$(selector_rows ts_b "$NARROW_SELECTOR" 0 200)

# The wide window makes the failing series time-eligible, so the probe finds a counterexample.
echo "wide window falls back: $(has_id_range ts_b "$NARROW_SELECTOR" 0 1000)"

echo "narrow window reuses that verdict: $([ "$(has_id_range ts_b "$NARROW_SELECTOR" 0 200)" = 0 ] && echo 1 || echo 0)"
echo "reuse returns the same rows: $([ "$(selector_rows ts_b "$NARROW_SELECTOR" 0 200)" = "$ROWS_FRESH" ] && echo 1 || echo 0)"

# Build the plan repeatedly in one call and find the first query that is probed again. A verdict
# that never expires would keep every value at 0.
MAX_USES=200
VERDICTS=$($CH -q "$(for _ in $(seq 1 $MAX_USES); do has_id_range_query ts_b "$NARROW_SELECTOR" 0 200; done)" | tr -d '\n')
echo "probed again within $MAX_USES queries: $([ "${VERDICTS%%1*}" != "$VERDICTS" ] && echo 1 || echo 0)"
echo "the verdict was reused at least once before that: $([ "${VERDICTS:0:1}" = 0 ] && echo 1 || echo 0)"
echo "rows unchanged after the verdict was retired: $([ "$(selector_rows ts_b "$NARROW_SELECTOR" 0 200)" = "$ROWS_FRESH" ] && echo 1 || echo 0)"
echo "rows in the narrow window:"
echo "$ROWS_FRESH"

$CH -q "DROP TABLE ts_b SYNC; DROP TABLE ts_a SYNC;"
