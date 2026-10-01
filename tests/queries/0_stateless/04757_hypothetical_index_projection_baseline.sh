#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_proj_baseline;
    CREATE TABLE t_hypo_proj_baseline (a UInt64, b UInt64, PROJECTION p_b (SELECT a, b ORDER BY b))
    ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_proj_baseline SELECT number, number % 100 FROM numbers(10000);
"

# The query is served from projection p_b, so a hypothetical index on the base table's parts
# cannot affect it. That used to fail the whole statement with NOT_IMPLEMENTED
echo "--- projection-served read: baseline is reported, candidate is not_applicable ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 42 SETTINGS optimize_use_projections = 1;
" 2>&1 | grep -oE "Baseline \(after PK \+ partition \+ existing indexes\):|est_bytes:.*|status: +not_applicable|reason: +.*" | awk '{$1=$1; print}'

echo "--- and the statement succeeds ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 42 SETTINGS optimize_use_projections = 1;
" > /dev/null 2>&1 && echo "no exception" || echo "FAILED"

echo "--- with projections off the same candidate is estimated as usual ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 42 SETTINGS optimize_use_projections = 0;
" 2>&1 | grep -oE 'status: +applicable|source: +empirical' | tr -s ' '

# Every plan shape that leaves no ReadFromMergeTree must report candidates instead of throwing.
# The projection rewrites and trivial count reach that state by different routes
echo "--- plans with no MergeTree read step still report candidates ---"
while IFS='|' read -r label query
do
    echo "$label"
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
        EXPLAIN WHATIF $query;
    " 2>&1 | grep -oE "status: +not_applicable|reason: +.*" | awk '{$1=$1; print}'
done <<'EOF'
trivial count|SELECT count() FROM t_hypo_proj_baseline SETTINGS optimize_trivial_count_query = 1
minmax_count projection|SELECT max(a) FROM t_hypo_proj_baseline SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
exact_count projection|SELECT count() FROM t_hypo_proj_baseline SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1
EOF

# A normal projection that selects no ranges may drop the read step or keep an empty one, which
# differs by plan; either way the statement must not fail
echo "--- a normal projection with no ranges does not fail ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 999999 SETTINGS optimize_use_projections = 1;
" > /dev/null 2>&1 && echo "no exception" || echo "FAILED"

# the no-scan path is still single-table only, and still honours force_data_skipping_indices
echo "--- a join is not silently reported as single-table ---"
$CLICKHOUSE_CLIENT -q "EXPLAIN WHATIF SELECT count() FROM t_hypo_proj_baseline AS x INNER JOIN t_hypo_proj_baseline AS y ON 0;" 2>&1 | grep -m1 -oE 'NOT_IMPLEMENTED'

echo "--- a forced index still fails when nothing is read ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_proj_baseline SETTINGS optimize_trivial_count_query = 1, force_data_skipping_indices = 'hi_b';
" 2>&1 | grep -m1 -oE 'INDEX_NOT_USED'

echo "--- but not when skip indexes are off, since a real read ignores the setting ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_proj_baseline SETTINGS optimize_trivial_count_query = 1, use_skip_indexes = 0, force_data_skipping_indices = 'hi_b';
" > /dev/null 2>&1 && echo "no exception" || echo "FAILED"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_proj_baseline;"
