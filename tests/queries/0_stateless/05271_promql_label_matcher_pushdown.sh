#!/usr/bin/env bash
# Tags: no-fasttest, long, no-tsan, no-msan
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag long: about 70 PromQL queries, which take more than 180s in the flaky check.
# Tag no-tsan: the 20-operator query needs more stack than the 5% that TSan allows.
# Tag no-msan: under MSan each of the 37 comparison queries takes about 20s to plan, so the test exceeds the 600s timeout.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --session_timezone=UTC"

$CLIENT -n -q "
DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('req', map('job', 'api', 'instance', 'i1'), [(toDateTime64(70, 3), 1), (toDateTime64(80, 3), 3), (toDateTime64(90, 3), 6), (toDateTime64(100, 3), 10), (toDateTime64(110, 3), 15), (toDateTime64(120, 3), 21), (toDateTime64(130, 3), 28)]),
    ('req', map('job', 'api', 'instance', 'i2'), [(toDateTime64(70, 3), 2), (toDateTime64(80, 3), 4), (toDateTime64(90, 3), 8), (toDateTime64(100, 3), 16), (toDateTime64(110, 3), 32), (toDateTime64(120, 3), 64), (toDateTime64(130, 3), 128)]),
    ('req', map('job', 'db', 'instance', 'i3'), [(toDateTime64(70, 3), 5), (toDateTime64(80, 3), 10), (toDateTime64(90, 3), 15), (toDateTime64(100, 3), 20), (toDateTime64(110, 3), 25), (toDateTime64(120, 3), 30), (toDateTime64(130, 3), 35)]),
    ('lim', map('job', 'api', 'instance', 'i1', 'team', 't1'), [(toDateTime64(70, 3), 10), (toDateTime64(80, 3), 20), (toDateTime64(90, 3), 30), (toDateTime64(100, 3), 40), (toDateTime64(110, 3), 50), (toDateTime64(120, 3), 60), (toDateTime64(130, 3), 70)]),
    ('lim', map('job', 'api', 'instance', 'i2', 'team', 't2'), [(toDateTime64(70, 3), 3), (toDateTime64(80, 3), 6), (toDateTime64(90, 3), 12), (toDateTime64(100, 3), 24), (toDateTime64(110, 3), 48), (toDateTime64(120, 3), 96), (toDateTime64(130, 3), 192)]),
    ('lim', map('job', 'api', 'instance', 'i3', 'team', 't3'), [(toDateTime64(70, 3), 7), (toDateTime64(80, 3), 14), (toDateTime64(90, 3), 21), (toDateTime64(100, 3), 28), (toDateTime64(110, 3), 35), (toDateTime64(120, 3), 42), (toDateTime64(130, 3), 49)]),
    ('lim', map('job', 'db', 'instance', 'i3', 'team', 't1'), [(toDateTime64(70, 3), 9), (toDateTime64(80, 3), 18), (toDateTime64(90, 3), 27), (toDateTime64(100, 3), 36), (toDateTime64(110, 3), 45), (toDateTime64(120, 3), 54), (toDateTime64(130, 3), 63)]),
    ('info', map('job', 'api', 'owner', 'alice'), [(toDateTime64(70, 3), 1), (toDateTime64(100, 3), 1), (toDateTime64(130, 3), 1)]),
    ('info', map('job', 'db', 'owner', 'bob'), [(toDateTime64(70, 3), 1), (toDateTime64(100, 3), 1), (toDateTime64(130, 3), 1)]),
    ('opt', map('instance', 'i1'), [(toDateTime64(70, 3), 1), (toDateTime64(80, 3), 2), (toDateTime64(90, 3), 4), (toDateTime64(100, 3), 7), (toDateTime64(110, 3), 11), (toDateTime64(120, 3), 16), (toDateTime64(130, 3), 22)]),
    ('opt', map('job', 'api', 'instance', 'i2'), [(toDateTime64(70, 3), 2), (toDateTime64(80, 3), 5), (toDateTime64(90, 3), 9), (toDateTime64(100, 3), 14), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 27), (toDateTime64(130, 3), 35)]),
    ('m1', map('a', '1'), [(toDateTime64(70, 3), 1), (toDateTime64(80, 3), 3), (toDateTime64(90, 3), 5), (toDateTime64(100, 3), 7), (toDateTime64(110, 3), 9), (toDateTime64(120, 3), 11), (toDateTime64(130, 3), 13)]),
    ('m1', map('a', '2'), [(toDateTime64(70, 3), 2), (toDateTime64(80, 3), 5), (toDateTime64(90, 3), 8), (toDateTime64(100, 3), 11), (toDateTime64(110, 3), 14), (toDateTime64(120, 3), 17), (toDateTime64(130, 3), 20)]),
    ('m2', map('a', '2'), [(toDateTime64(70, 3), 3), (toDateTime64(80, 3), 4), (toDateTime64(90, 3), 5), (toDateTime64(100, 3), 6), (toDateTime64(110, 3), 7), (toDateTime64(120, 3), 8), (toDateTime64(130, 3), 9)]);
"

# `label_replace` of a label nobody has changes nothing but stops the pushdown, so it gives the result without it.
function b()
{
    echo "label_replace($1, \"nx\", \"\", \"nx\", \"\")"
}

# Prints whether the query returns the same series as the query without the pushdown, and how many series it returns.
# The values are rounded because a sum of three or more series can differ in the last bit.
function check()
{
    $CLIENT -q "
        SELECT '$1', a = b, length(a)
        FROM
        (
            SELECT
                (SELECT groupArray((tags, arrayMap(s -> (s.1, round(s.2, 9)), samples))) FROM (SELECT * FROM prometheusQueryRange('prometheus', '$2', 100, 130, 10) ORDER BY tags)) AS a,
                (SELECT groupArray((tags, arrayMap(s -> (s.1, round(s.2, 9)), samples))) FROM (SELECT * FROM prometheusQueryRange('prometheus', '$3', 100, 130, 10) ORDER BY tags)) AS b
        )"
}

echo "-- same result with and without the pushdown"
check on 'rate(req{job="api"}[30s]) / on(job, instance) rate(lim[30s])' "$(b 'rate(req{job="api"}[30s])') / on(job, instance) $(b 'rate(lim[30s])')"
check regex 'rate(req{job=~"a.*"}[30s]) / on(job, instance) rate(lim[30s])' "$(b 'rate(req{job=~"a.*"}[30s])') / on(job, instance) $(b 'rate(lim[30s])')"
check not_equal 'rate(req{job!="db"}[30s]) / on(job, instance) rate(lim[30s])' "$(b 'rate(req{job!="db"}[30s])') / on(job, instance) $(b 'rate(lim[30s])')"
check both_sides 'req{job="api"} / on(job, instance) lim{instance="i2"}' "$(b 'req{job="api"}') / on(job, instance) $(b 'lim{instance="i2"}')"
check ignoring 'req{job="api"} / ignoring(team) lim' "$(b 'req{job="api"}') / ignoring(team) $(b 'lim')"
check right_to_left 'req / ignoring(team) lim{team="t1", job="db"}' "$(b 'req') / ignoring(team) $(b 'lim{team="t1", job="db"}')"
check label_not_in_on 'req{job="db"} / on(instance) lim{job="api"}' "$(b 'req{job="db"}') / on(instance) $(b 'lim{job="api"}')"
check sum_by 'sum by (job) (rate(req{job="api"}[30s])) / on(job) sum by (job) (rate(lim[30s]))' "$(b 'sum by (job) (rate(req{job="api"}[30s]))') / on(job) $(b 'sum by (job) (rate(lim[30s]))')"
check sum_by_other_label 'sum by (instance) (req{job="db"}) / on(instance) lim{team="t3"}' "$(b 'sum by (instance) (req{job="db"})') / on(instance) $(b 'lim{team="t3"}')"
check sum_without 'sum without (instance) (req{job="api"}) / sum without (instance, team) (lim)' "$(b 'sum without (instance) (req{job="api"})') / $(b 'sum without (instance, team) (lim)')"
check sum 'sum(req{job="api"}) / on() sum(lim)' "$(b 'sum(req{job="api"})') / on() $(b 'sum(lim)')"
check topk_by 'topk by (job) (1, req{job="api"}) / ignoring(team) lim' "$(b 'topk by (job) (1, req{job="api"})') / ignoring(team) $(b 'lim')"
check topk 'topk(1, req) / on(job, instance) lim{job="db"}' "$(b 'topk(1, req)') / on(job, instance) $(b 'lim{job="db"}')"
check group_left 'req{job="api"} * on(job) group_left(owner) info' "$(b 'req{job="api"}') * on(job) group_left(owner) $(b 'info')"
check group_right 'info{job="db"} * on(job) group_right(owner) req' "$(b 'info{job="db"}') * on(job) group_right(owner) $(b 'req')"
check and 'req{job="api"} and on(job, instance) lim' "$(b 'req{job="api"}') and on(job, instance) $(b 'lim')"
check and_right_to_left 'req and on(job) lim{job="db"}' "$(b 'req') and on(job) $(b 'lim{job="db"}')"
check unless 'req{job="api"} unless on(job, instance) lim{team="t2"}' "$(b 'req{job="api"}') unless on(job, instance) $(b 'lim{team="t2"}')"
check unless_right_to_left 'req unless on(job) lim{job="db"}' "$(b 'req') unless on(job) $(b 'lim{job="db"}')"
check or 'req{job="api"} or on(job, instance) lim' "$(b 'req{job="api"}') or on(job, instance) $(b 'lim')"
check offset 'rate(req{job="api"}[30s] offset 10s) / on(job, instance) rate(lim[30s])' "$(b 'rate(req{job="api"}[30s] offset 10s)') / on(job, instance) $(b 'rate(lim[30s])')"
check subquery 'max_over_time(rate(req{job="api"}[20s])[30s:10s]) / on(job, instance) max_over_time(rate(lim[20s])[30s:10s])' "$(b 'max_over_time(rate(req{job="api"}[20s])[30s:10s])') / on(job, instance) $(b 'max_over_time(rate(lim[20s])[30s:10s])')"
check scalar '100 * rate(req{job="api"}[30s]) / on(job, instance) rate(lim[30s])' "$(b '100 * rate(req{job="api"}[30s])') / on(job, instance) $(b 'rate(lim[30s])')"
check quantile_over_time 'quantile_over_time(0.5, req{job="api"}[30s]) / on(job, instance) quantile_over_time(0.5, lim[30s])' "$(b 'quantile_over_time(0.5, req{job="api"}[30s])') / on(job, instance) $(b 'quantile_over_time(0.5, lim[30s])')"
check predict_linear 'predict_linear(req{job="api"}[30s], 10) / on(job, instance) predict_linear(lim[30s], 10)' "$(b 'predict_linear(req{job="api"}[30s], 10)') / on(job, instance) $(b 'predict_linear(lim[30s], 10)')"
check comparison 'req{job="api"} > bool on(job, instance) lim' "$(b 'req{job="api"}') > bool on(job, instance) $(b 'lim')"
check comparison_several_metrics '({__name__=~"req|opt"} > 0) and on(job, instance) lim{job="api"}' "$(b '{__name__=~"req|opt"} > 0') and on(job, instance) $(b 'lim{job="api"}')"
check contradiction 'req{job="api"} / on(job, instance) lim{job="db"}' "$(b 'req{job="api"}') / on(job, instance) $(b 'lim{job="db"}')"
check label_replace 'label_replace(req{job="db"}, "job", "api", "", "") / on(job, instance) lim' "$(b 'label_replace(req{job="db"}, "job", "api", "", "")') / on(job, instance) $(b 'lim')"
check nested '(req{job="api"} - ignoring(team) lim) / ignoring(team) lim' "($(b 'req{job="api"}') - ignoring(team) $(b 'lim')) / ignoring(team) $(b 'lim')"
check nested_unless '(req unless on(job) lim{job="db"}) / ignoring(team) lim' "($(b 'req') unless on(job) $(b 'lim{job="db"}')) / ignoring(team) $(b 'lim')"
check empty_matcher 'opt{job=""} / on(job, instance) rate(opt[30s])' "$(b 'opt{job=""}') / on(job, instance) $(b 'rate(opt[30s])')"
check regex_matching_empty 'opt{job=~"api|"} / on(job, instance) rate(opt[30s])' "$(b 'opt{job=~"api|"}') / on(job, instance) $(b 'rate(opt[30s])')"
check not_empty 'opt{job!=""} / on(job, instance) rate(opt[30s])' "$(b 'opt{job!=""}') / on(job, instance) $(b 'rate(opt[30s])')"
check count_values 'count_values("job", req) * on(job) group_left info{job="api"}' "$(b 'count_values("job", req)') * on(job) group_left $(b 'info{job="api"}')"
check absent 'absent(req{instance="i3"}) / ignoring(team) lim{job="api", instance="i3"}' "$(b 'absent(req{instance="i3"})') / ignoring(team) $(b 'lim{job="api", instance="i3"}')"
check fusion 'sum by (job) (req{job="api"}) / sum by (job) (req)' "$(b 'sum by (job) (req{job="api"})') / $(b 'sum by (job) (req)')"

echo "-- a duplicate series in a matched group is still an error"
$CLIENT -q "SELECT * FROM prometheusQueryRange('prometheus', 'req{instance=\"i3\"} / on(instance) lim', 100, 130, 10)" 2>&1 | grep -o -m1 "CANNOT_EXECUTE_PROMQL_QUERY"
$CLIENT -q "SELECT * FROM prometheusQueryRange('prometheus', '(lim / on(instance) req) / on(instance) req{instance=\"i1\"}', 100, 130, 10)" 2>&1 | grep -o -m1 "CANNOT_EXECUTE_PROMQL_QUERY"

echo "-- as in Prometheus, a duplicate series on the one side or after dropping the metric name is an error in any group"
$CLIENT -q "SELECT * FROM prometheusQueryRange('prometheus', 'req{instance=\"i1\"} / on(instance) lim', 100, 130, 10)" 2>&1 | grep -o -m1 "CANNOT_EXECUTE_PROMQL_QUERY"
$CLIENT -q "SELECT * FROM prometheusQueryRange('prometheus', 'req{instance=\"i1\"} / on(instance) group_left lim', 100, 130, 10)" 2>&1 | grep -o -m1 "CANNOT_EXECUTE_PROMQL_QUERY"
$CLIENT -q "SELECT * FROM prometheusQueryRange('prometheus', 'rate({__name__=~\"m1|m2\"}[30s]) / on(a) m1{a=\"1\"}', 100, 130, 10)" 2>&1 | grep -o -m1 "CANNOT_EXECUTE_PROMQL_QUERY"

echo "-- as in Prometheus, duplicate left series of a one-to-one match are an error only in a matched group"
$CLIENT -q "SELECT * FROM prometheusQueryRange('prometheus', 'lim / on(instance) req{instance=\"i1\"}', 100, 130, 10) ORDER BY tags"

# Prints the label values each selector filters by. EXPLAIN keeps the selector filters in the plan only for an empty table.
function selectors()
{
    echo "$1"
    $CLIENT -q "
        SELECT DISTINCT arrayStringConcat(extractAll(explain, '= \'([a-z0-9]+)\''), ' ') AS filter
        FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus_empty', '$1', 100, 130, 10))
        WHERE explain LIKE '%metric_name%'
        ORDER BY filter"
}

$CLIENT -n -q "DROP TABLE IF EXISTS prometheus_empty; CREATE TABLE prometheus_empty ENGINE = TimeSeries;"

echo "-- the selectors read"
selectors 'rate(lim[30s]) / on(instance) rate(req{instance="i1"}[30s])'
selectors 'rate(req{instance="i1"}[30s]) / on(instance) rate(lim[30s])'
selectors 'req{job="api"} / on(job, instance) lim{instance="i2"}'
selectors 'req{job="api"} / ignoring(team) lim{team="t1"}'
selectors 'sum by (job) (rate(req{job="api"}[30s])) / on(job) sum by (job) (rate(lim[30s]))'
selectors 'sum by (instance) (req{job="api"}) / on(instance) lim{instance="i1"}'
selectors 'req unless on(job) lim{job="db"}'
selectors 'req{job="api"} unless on(job) lim'
selectors 'req{job="api"} or on(job, instance) lim'
selectors 'label_replace(req{job="api"}, "job", "db", "", "") / on(job) lim'
selectors 'count_values("job", req) * on(job) group_left info{job="api"}'
selectors 'absent(req{instance="i3"}) / ignoring(team) lim{job="api"}'
selectors 'lim / on(job) group_left sum by (job) (req{job="api"})'
selectors '(-lim) / on(job) group_left sum by (job) (req{job="api"})'
selectors 'abs(lim) / on(job) group_left sum by (job) (req{job="api"})'
selectors '({__name__=~"req|opt"} > 0) and on(job) lim{job="api"}'
selectors '({__name__=~"req|opt"} > bool 0) and on(job) lim{job="api"}'

# Prints $1 `lim` selectors joined by `and` as a balanced tree, to keep the SQL nesting shallow.
function and_of_lim()
{
    if [ "$1" -eq 1 ]; then echo "lim"; return; fi
    echo "($(and_of_lim $(($1 / 2))) and $(and_of_lim $(($1 - $1 / 2))))"
}

echo "-- more than 20 binary operators turn the pushdown off"
selectors "$(and_of_lim 20) / on(job) req{job=\"api\"}"
selectors "$(and_of_lim 21) / on(job) req{job=\"api\"}"

echo "-- the setting turns the pushdown off"
CLIENT="$CLIENT --promql_push_down_label_matchers=0" selectors 'rate(lim[30s]) / on(instance) rate(req{instance="i1"}[30s])'

# Prints how many selectors the query reads and how many joins it makes.
function reads()
{
    $CLIENT -q "
        SELECT countIf(explain LIKE '%metric_name = %'), countIf(explain LIKE '%Join (%')
        FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus_empty', '$1', 100, 130, 10))"
}

echo "-- the two sides become the same, so they are read once"
reads 'sum by (job) (req{job="api"}) / sum by (job) (req)'
CLIENT="$CLIENT --promql_push_down_label_matchers=0" reads 'sum by (job) (req{job="api"}) / sum by (job) (req)'

$CLIENT -q "DROP TABLE prometheus_empty"
$CLIENT -q "DROP TABLE prometheus"
