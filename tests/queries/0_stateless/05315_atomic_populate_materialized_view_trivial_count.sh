#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database
# - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
#   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
# - no-fasttest - a test that must run alone is kept out of the fast test.
# - no-replicated-database - the CREATE would go through the replicated DDL log, where the population is
#   always the legacy one, so the atomic path (and its fail point) is not exercised there.

# The atomic `CREATE MATERIALIZED VIEW ... POPULATE` subscribes the view to its source and captures a
# snapshot of the source's data before the population runs. A row inserted after that is pushed to the
# view live, so the population must not count it again - even when the view's query is a bare `count()`,
# which can be answered from table metadata instead of a read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_05315;
    DROP TABLE IF EXISTS src_05315;
    CREATE TABLE src_05315 (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO src_05315 SELECT number FROM numbers(10);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_population"

$CLICKHOUSE_CLIENT --optimize_trivial_count_query 1 --materialized_views_populate_atomically 1 -q "
    CREATE MATERIALIZED VIEW mv_05315 ENGINE = MergeTree ORDER BY c POPULATE AS SELECT count() AS c FROM src_05315
" &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_population PAUSE"

# Lands after the snapshot, so it reaches the view only through the live push.
$CLICKHOUSE_CLIENT -q "INSERT INTO src_05315 SELECT number + 10 FROM numbers(5)"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_population"
wait $CREATE_PID

# 15 = 10 rows from the population + 5 from the live push; 20 if the population also counted the 5.
$CLICKHOUSE_CLIENT -q "SELECT 'rows counted by the view:', sum(c) FROM mv_05315"

# Same for a filtered `count()`, which `optimize_trivial_count_with_sparsity_filter` answers from per-part counters of the live table.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_05315;
    DROP TABLE IF EXISTS src_05315;
    CREATE TABLE src_05315 (n UInt64) ENGINE = MergeTree ORDER BY n
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;
    INSERT INTO src_05315 SELECT number FROM numbers(10);
    SELECT 'sparsity filter plan:', countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
    FROM (EXPLAIN SELECT count() FROM src_05315 WHERE n != 0 SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT atomic_populate_pause_before_population"

$CLICKHOUSE_CLIENT --optimize_trivial_count_query 1 --optimize_trivial_count_with_sparsity_filter 1 --materialized_views_populate_atomically 1 -q "
    CREATE MATERIALIZED VIEW mv_05315 ENGINE = MergeTree ORDER BY c POPULATE AS SELECT count() AS c FROM src_05315 WHERE n != 0
" &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT atomic_populate_pause_before_population PAUSE"
$CLICKHOUSE_CLIENT -q "INSERT INTO src_05315 SELECT number + 10 FROM numbers(5)"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT atomic_populate_pause_before_population"
wait $CREATE_PID

# 14 = 9 non-zero rows from the population + 5 from the live push; 19 if the population also counted the 5.
$CLICKHOUSE_CLIENT -q "SELECT 'sparsity filter rows counted by the view:', sum(c) FROM mv_05315"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05315;
    DROP TABLE src_05315;
"
