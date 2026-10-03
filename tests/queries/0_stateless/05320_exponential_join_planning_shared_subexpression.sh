#!/usr/bin/env bash
# Random settings limits: query_plan_optimize_join_order_limit=(3, None)
# A JOIN key built by nested subqueries that each use the previous column twice (`x + x`) has 2^36 paths
# through its expression. Planning the join must not take time proportional to the number of paths.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LEVELS=6
STEPS=6

projection() {
    local seed=$1 level=$2 p
    p="$seed + $seed AS a${level}_1"
    for j in $(seq 2 $STEPS); do
        p="$p, a${level}_$((j - 1)) + a${level}_$((j - 1)) AS a${level}_$j"
    done
    echo "$p"
}

# The chain is computed on the preserved side of the LEFT JOIN, so all three tables are reordered together
# and the chain becomes the key of the INNER JOIN.
query="SELECT $(projection 'r.val' 1) FROM (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r"
query="$query LEFT JOIN (SELECT number AS id FROM numbers(2)) AS l ON l.id = r.id"
for i in $(seq 2 $LEVELS); do
    query="SELECT $(projection "a$((i - 1))_$STEPS" "$i") FROM ($query)"
done
key="g.a${LEVELS}_${STEPS}"
join="FROM ($query) AS g INNER JOIN (SELECT 0 AS val) AS k ON $key = k.val"

# The Stress test re-runs fuzzed variants of every query; one that turns a join of this key into an INNER JOIN with runtime
# filters runs out of memory in filter push-down (#123724, not fixed here), so the server-side fuzzer is off for this test.
${CLICKHOUSE_CLIENT} --ast_fuzzer_runs 0 --join_use_nulls 0 --enable_join_runtime_filters 0 --query "SELECT count() $join"
# With join_use_nulls the planner also derives which tables the key rejects NULLs from.
${CLICKHOUSE_CLIENT} --ast_fuzzer_runs 0 --join_use_nulls 1 --enable_join_runtime_filters 0 --query "SELECT count() $join"
# The key is also an output of the join.
${CLICKHOUSE_CLIENT} --ast_fuzzer_runs 0 --join_use_nulls 0 --enable_join_runtime_filters 0 --query "SELECT count(), sum($key) $join"
# A LEFT JOIN on the key gets no runtime filter, so it is also planned with the default settings.
${CLICKHOUSE_CLIENT} --ast_fuzzer_runs 0 --join_use_nulls 0 --query "SELECT count() FROM ($query) AS g LEFT JOIN (SELECT 0 AS val) AS k ON $key = k.val"
