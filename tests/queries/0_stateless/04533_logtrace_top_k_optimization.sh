#!/usr/bin/env bash
# Tags: race

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `logTrace` (and other stateful functions) must observe every input block even when the query has
# `ORDER BY ... LIMIT` over `MergeTree`, which enables the top-K optimizations. `optimizeTopK` installs
# a `__topKFilter` prewhere (and, with a skip index, minmax granule pruning) on the read that drops
# source rows as the sort threshold stabilizes; `topKThroughJoin` pushes the sort + limit below a
# `JOIN`, truncating the preserved side. Either would feed the stateful function only the surviving
# candidate rows. The `hasStatefulFunctions()` guards keep these optimizations from firing.

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04533, t_04533_r"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_04533 (key UInt64, payload UInt64) ENGINE = MergeTree PARTITION BY (key % 8) ORDER BY tuple() SETTINGS index_granularity = 8"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_04533_r (key UInt64, val UInt64) ENGINE = MergeTree PARTITION BY (key % 8) ORDER BY tuple() SETTINGS index_granularity = 8"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04533 SELECT number, number FROM numbers(100)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04533_r SELECT number, number FROM numbers(100)"

# --- optimizeTopK (prewhere filter / skip-index granule pruning) ---
# Self-calibrating trace-message count: enabling the top-K optimizations must not reduce how many
# blocks reach `logTrace`. The table is partitioned so the read spans several blocks regardless of the
# harness's randomized `index_granularity` / `max_block_size` (parts in different partitions never
# merge), so the baseline is > 1.
count_topk() {
    # $1 = tag, $2 = 0 (disabled) | 1 (enabled)
    # `enable_parallel_replicas` is pinned off: the top-K optimizations do not run on a distributed
    # plan (`optimizeTopK` bails on `make_distributed_plan`), and a remote replica's `logTrace`
    # messages are not delivered to the client, which would make the count unobservable.
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query="
        SELECT logTrace('$1'), payload FROM t_04533 ORDER BY key LIMIT 1
        SETTINGS max_block_size = 1, max_threads = 1, enable_parallel_replicas = 0,
                 use_top_k_dynamic_filtering = $2, use_skip_indexes_for_top_k = $2 FORMAT Null
    " 2>&1 | grep -c "FunctionLogTrace: $1"
}
base=$(count_topk topk_off 0)
kept=$(count_topk topk_on 1)
if [ "$kept" = "$base" ] && [ "$base" -gt 1 ]; then echo "OK"; else echo "FAIL kept=$kept base=$base"; fi

# --- topKThroughJoin (sort + limit pushed below the JOIN) ---
# A `LEFT JOIN` squashes its small output into a single block, so a message count does not distinguish
# the two plans here. Test the guard structurally instead: pushing the sort below the join adds a
# second `Sorting` step to the plan. The join algorithm is pinned to a non-spilling hash join so the
# optimization is eligible regardless of the harness's randomized join settings. With the guard, the
# stateful query keeps its single top-level sort whether or not the optimization is enabled, while the
# non-stateful control gains the pushed-down sort - proving the check would catch a regression.
sort_count() {
    # $1 = select list, $2 = query_plan_top_k_through_join (0|1)
    ${CLICKHOUSE_CLIENT} --query="
        EXPLAIN SELECT $1 FROM t_04533 AS l LEFT JOIN t_04533_r AS r ON l.key = r.key ORDER BY l.key LIMIT 1
        SETTINGS enable_analyzer = 1, join_algorithm = 'hash', enable_parallel_replicas = 0,
                 max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
                 use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0,
                 query_plan_top_k_through_join = $2
    " 2>&1 | grep -ciE "Sorting"
}
lt_off=$(sort_count "logTrace('j'), l.payload" 0)
lt_on=$(sort_count "logTrace('j'), l.payload" 1)
ns_off=$(sort_count "r.val, l.payload" 0)
ns_on=$(sort_count "r.val, l.payload" 1)
if [ "$lt_on" = "$lt_off" ] && [ "$ns_on" -gt "$ns_off" ]; then echo "OK"; else echo "FAIL lt=$lt_off/$lt_on ns=$ns_off/$ns_on"; fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04533, t_04533_r"
