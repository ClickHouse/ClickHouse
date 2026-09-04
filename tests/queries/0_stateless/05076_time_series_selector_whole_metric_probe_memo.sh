#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# `timeSeriesSelector` gates its whole-metric primary-key range on a probe query over the tags
# table, and the probe's counterexample-found verdict is reused for a bounded number of later
# queries of the same shape. Three properties of that reuse are checked here.
#
# A. The opposite verdict is never reused. A selector that matches the whole metric today stops
#    matching it once a series is written with an id outside the metric's range; reusing the old
#    verdict would keep emitting the range, which filters the out-of-range series out of the result.
# B. A reused verdict is retired after a bounded number of uses, so a selector that becomes
#    whole-metric again is probed again. The reuse key excludes the query's time bounds, so a
#    narrowed window - whose own probe finds no counterexample - reuses the entry until it expires;
#    that is what this scenario uses to make the retirement observable.
# C. Reuse skips the probe query itself. A and B read the emitted plan, which is the same whether
#    the probe ran or its result was reused, so on their own they would stay green for an
#    implementation that probes and then discards the answer.
#
# The range conditions carry the max-UUID literal, which is how the emission is detected below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The test runner can enable the query result cache, and its key ignores `log_comment`. Every
# scenario below needs a byte-identical query to answer differently the second time, which a cache
# hit defeats. On `$CH` so it also covers statements a later scenario adds.
CH="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 --session_timezone UTC --use_query_cache 0"

# Scenario B counts statements to count verdict uses, which needs one plan build per statement. A
# non-zero `automatic_parallel_replicas_mode` builds a second complete plan to decide whether
# parallel replicas pay off, and that second build consumes a second use of the same verdict.
ONE_PLAN_BUILD='SETTINGS automatic_parallel_replicas_mode = 0'

# Prints 1 if the built plan carries the whole-metric id range, 0 otherwise. Each call builds the
# plan once, i.e. consumes exactly one probe verdict.
has_id_range_query() {
    echo "SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%'
          FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
                FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector($1, '$2', $3, $4) $ONE_PLAN_BUILD));"
}

has_id_range() {
    $CH -q "$(has_id_range_query "$1" "$2" "$3" "$4")"
}

selector_rows() {
    $CH -q "SELECT timestamp, value FROM timeSeriesSelector($1, '$2', $3, $4) ORDER BY value, timestamp $ONE_PLAN_BUILD;"
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
MAX_USES=40
VERDICTS=$($CH -q "$(for _ in $(seq 1 $MAX_USES); do has_id_range_query ts_b "$NARROW_SELECTOR" 0 200; done)" | tr -d '\n')
REUSED_IN_LOOP=${VERDICTS%%1*}
echo "probed again within $MAX_USES queries: $([ "$REUSED_IN_LOOP" != "$VERDICTS" ] && echo 1 || echo 0)"
echo "plan builds that reused the verdict before the re-probe: $(( 2 + ${#REUSED_IN_LOOP} ))"
echo "rows unchanged after the verdict was retired: $([ "$(selector_rows ts_b "$NARROW_SELECTOR" 0 200)" = "$ROWS_FRESH" ] && echo 1 || echo 0)"
echo "rows in the narrow window:"
echo "$ROWS_FRESH"

echo "-- C. reuse skips the probe's work, not just its emitted range"

# Its own table, so this scenario cannot consume the reuse budget scenario B measures.
$CH -q "
DROP TABLE IF EXISTS ts_c SYNC;
CREATE TABLE ts_c ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
INSERT INTO ts_c (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);
"

# The series that fails the matcher is time-eligible, so the probe finds a counterexample and both
# queries fall back to the id set condition. The query text is identical and the data does not
# change between them, so their plans and their own reads are identical and the whole difference in
# marks is the probe's read of the tags table - which the second query must not repeat.
for comment in 05076_probe_1 05076_probe_2; do
    $CH --log_comment "$comment" -q \
        "SELECT sum(value) FROM timeSeriesSelector(ts_c, 'foo{env=\"prod\"}', 0, 1000) FORMAT Null"
done

$CH -q "SYSTEM FLUSH LOGS query_log;"

marks_of() {
    $CH -q "SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
            WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
                  AND log_comment = '$1'
            ORDER BY event_time_microseconds DESC LIMIT 1;"
}
MARKS_PROBED=$(marks_of 05076_probe_1)
MARKS_REUSED=$(marks_of 05076_probe_2)

echo "reuse reads fewer marks than the probe: $([ "$MARKS_PROBED" -gt "$MARKS_REUSED" ] && echo 1 || echo 0)"
# Positive control: without it two zeros - a query that read nothing at all - would also pass above.
echo "the reusing query still reads marks of its own: $([ "$MARKS_REUSED" -gt 0 ] && echo 1 || echo 0)"

$CH -q "DROP TABLE ts_c SYNC; DROP TABLE ts_b SYNC; DROP TABLE ts_a SYNC;"
