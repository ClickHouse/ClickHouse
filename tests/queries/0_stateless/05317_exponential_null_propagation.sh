#!/usr/bin/env bash
# `mergeExpressions` folds the projections below into one DAG holding a long chain of `plus` nodes
# whose two arguments are the same node, so walking every path instead of every node costs
# O(2^depth).

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

query="SELECT $(projection 'r.val' 1) FROM (SELECT number AS id FROM numbers(2)) AS l"
query="$query LEFT JOIN (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r ON l.id = r.id"
for i in $(seq 2 $LEVELS); do
    query="SELECT $(projection "a$((i - 1))_$STEPS" "$i") FROM ($query)"
done

# Should finish in milliseconds. In previous versions planning doubled in time with every step.
${CLICKHOUSE_CLIENT} --join_use_nulls 1 --enable_join_runtime_filters 0 \
    --query "SELECT count() FROM ($query) AS g INNER JOIN (SELECT 0 AS val) AS k ON g.a${LEVELS}_${STEPS} = k.val"
