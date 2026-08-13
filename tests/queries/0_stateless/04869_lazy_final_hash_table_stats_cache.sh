#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The FINAL-replacing aggregation of the lazy FINAL optimization used to run with
# default-constructed StatsCollectingParams, i.e. max_entries_for_hash_table_stats = 0. The
# process-wide hash-table statistics cache is created by the first query that touches it, with
# that value as its capacity - so when a lazy FINAL query was the first aggregation in a freshly
# started server, it created the cache with zero capacity and permanently disabled hash-table
# stats (and so preallocation) for every later aggregation in the process.
#
# clickhouse-local gives a fresh process, which makes the first-touch scenario deterministic:
# the lazy FINAL query runs first and must not poison the cache for the victim aggregation.
# The victim runs twice: the first run populates the stats entry, the second one must preallocate
# from it, observed through the process-global AggregationPreallocatedElementsInHashTables event.
# The victim's group count (650e3) must stay above the 500e3 lower bound under which getSizeHint
# does not preallocate at all. External aggregation would stop the stats collection, so both
# spill thresholds are pinned to 0 (see 04625_hash_table_sizes_stats_table_expression_modifiers).

$CLICKHOUSE_LOCAL \
    --collect_hash_table_stats_during_aggregation=1 \
    --max_size_to_preallocate_for_aggregation=1000000000000 \
    --max_threads=1 \
    --max_bytes_before_external_group_by=0 \
    --max_bytes_ratio_before_external_group_by=0 \
    -q "
    CREATE TABLE t_lazy_final (key UInt64, version UInt64, status String, payload String)
    ENGINE = ReplacingMergeTree(version) ORDER BY key;

    SYSTEM STOP MERGES t_lazy_final;
    INSERT INTO t_lazy_final SELECT number, 1, 'target', repeat('x', 10) FROM numbers(1000);
    INSERT INTO t_lazy_final SELECT number, 2, 'target', repeat('x', 10) FROM numbers(1000);

    -- The lazy FINAL dedup aggregation is the first aggregation in this process.
    SELECT count(), sum(length(payload)) FROM t_lazy_final FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

    CREATE TABLE t_victim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_victim SELECT number, number FROM numbers(650000);

    SELECT v FROM t_victim GROUP BY v FORMAT Null;
    SELECT v FROM t_victim GROUP BY v FORMAT Null;

    SELECT value FROM system.events WHERE event = 'AggregationPreallocatedElementsInHashTables';
"
