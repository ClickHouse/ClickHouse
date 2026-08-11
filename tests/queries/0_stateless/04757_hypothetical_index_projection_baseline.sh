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
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 42;
" 2>&1 | grep -oE "Baseline \(after PK \+ partition \+ existing indexes\):|status: +not_applicable|served from projection 'p_b'" | tr -s ' '

echo "--- and the statement succeeds ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 42;
" > /dev/null 2>&1 && echo "no exception" || echo "FAILED"

echo "--- with projections off the same candidate is estimated as usual ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_proj_baseline (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj_baseline WHERE b = 42 SETTINGS optimize_use_projections = 0;
" 2>&1 | grep -oE 'status: +applicable|source: +empirical' | tr -s ' '

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_proj_baseline;"
