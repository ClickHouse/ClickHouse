#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stateful function (e.g. `logTrace`, `neighbor`) in a reader-side filter (an explicit `PREWHERE`, or a
# row-level policy filter) or in a `FilterStep` must observe every input block. `optimizePrimaryKeyConditionAndLimit`
# composes such filters into the index analysis - pruning partitions/granules by their deterministic
# conjuncts - and propagates the outer `LIMIT` into the source, both of which would shrink the stream
# the stateful function sees. The `hasStatefulFunctions` fences keep the reader untouched instead.

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04542"
${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE t_04542 (key UInt64, v UInt64) ENGINE = MergeTree
    PARTITION BY intDiv(key, 16) ORDER BY key SETTINGS index_granularity = 8"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04542 SELECT number, number FROM numbers(128)"

# Self-calibrating trace-message count: adding a partition/PK-prunable conjunct (`key < 16`) next to the
# stateful function must not reduce how many blocks reach `logTrace` - without the fence, the filter
# would be composed into the index analysis and the pruning would drop 7 of the 8 partitions before the
# stateful part runs. The table is partitioned so the read spans several blocks regardless of the
# harness's randomized `max_block_size` (parts in different partitions never merge), so the baseline
# is > 1. `enable_parallel_replicas` is pinned off because a remote replica's `logTrace` messages are
# not delivered to the client, which would make the count unobservable.
count_lines() {
    # $1 = tag, $2 = query
    ${CLICKHOUSE_CLIENT} --send_logs_level=trace --query="$2 FORMAT Null" 2>&1 | grep -c "FunctionLogTrace: $1"
}

# --- the stateful predicate is an explicit PREWHERE (reader-side) ---
pin="max_threads = 1, enable_parallel_replicas = 0"
base=$(count_lines pw_base "SELECT count() FROM t_04542 PREWHERE logTrace('pw_base') = 0 SETTINGS $pin")
kept=$(count_lines pw_key "SELECT count() FROM t_04542 PREWHERE logTrace('pw_key') = 0 AND key < 16 SETTINGS $pin")
if [ "$kept" = "$base" ] && [ "$base" -gt 1 ]; then echo "OK"; else echo "FAIL kept=$kept base=$base"; fi

# --- the stateful predicate is a visible FilterStep (`optimize_move_to_prewhere = 0` keeps WHERE as a filter) ---
base=$(count_lines w_base "SELECT count() FROM t_04542 WHERE logTrace('w_base') = 0 SETTINGS optimize_move_to_prewhere = 0, $pin")
kept=$(count_lines w_key "SELECT count() FROM t_04542 WHERE logTrace('w_key') = 0 AND key < 16 SETTINGS optimize_move_to_prewhere = 0, $pin")
if [ "$kept" = "$base" ] && [ "$base" -gt 1 ]; then echo "OK"; else echo "FAIL kept=$kept base=$base"; fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04542"
