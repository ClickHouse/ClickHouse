#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stateful function (e.g. `logTrace`, `neighbor`) in an explicit `PREWHERE` executes inside the reader,
# below both top-K paths, where the `ExpressionStep`/`FilterStep` guards of `tryOptimizeTopK` cannot see
# it. The dynamic `__topKFilter` path is disabled whenever a prewhere exists, but skip-index top-k
# pruning would still drop marks before the prewhere runs, so the stateful predicate would observe only
# the shortlisted rows. The reader-side `hasStatefulFunctions` fence keeps the optimization from firing.

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04543"
${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE t_04543 (key UInt64, payload UInt64, INDEX mm_key key TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY (key % 8) ORDER BY tuple() SETTINGS index_granularity = 8"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04543 SELECT number, number FROM numbers(100)"

# Self-calibrating trace-message count: enabling skip-index top-k must not reduce how many blocks reach
# the `logTrace` inside the PREWHERE. The sort below consumes its whole input, so the count equals the
# number of blocks the reader produces. The table is partitioned so the read spans several blocks
# regardless of the harness's randomized `max_block_size` (parts in different partitions never merge),
# so the baseline is > 1. `enable_parallel_replicas` is pinned off: the top-K optimizations do not run
# on a distributed plan, and a remote replica's `logTrace` messages are not delivered to the client.
count_topk() {
    # $1 = tag, $2 = 0 (disabled) | 1 (enabled)
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query="
        SELECT payload FROM t_04543 PREWHERE logTrace('$1') = 0 ORDER BY key LIMIT 1
        SETTINGS max_block_size = 1, max_threads = 1, enable_parallel_replicas = 0,
                 use_skip_indexes_for_top_k = $2, use_top_k_dynamic_filtering = 0 FORMAT Null
    " 2>&1 | grep -c "FunctionLogTrace: $1"
}
base=$(count_topk pwtopk_off 0)
kept=$(count_topk pwtopk_on 1)
if [ "$kept" = "$base" ] && [ "$base" -gt 1 ]; then echo "OK"; else echo "FAIL kept=$kept base=$base"; fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04543"
