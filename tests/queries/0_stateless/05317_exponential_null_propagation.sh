#!/usr/bin/env bash
# Planning a join whose key is built by nested subqueries that each use the previous column twice
# must stay linear in the DAG. `mergeExpressions` folds the projections into one DAG where `x + x`
# holds the same child node twice, so walking every path instead of every node costs O(2^depth).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="SELECT r.val + r.val AS x1 FROM (SELECT number AS id FROM numbers(2)) AS l LEFT JOIN (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r ON l.id = r.id"
for i in $(seq 2 30); do
    query="SELECT x$((i - 1)) + x$((i - 1)) AS x$i FROM ($query)"
done

# Should finish in milliseconds. In previous versions planning this query doubled in time with every nesting level.
${CLICKHOUSE_CLIENT} --join_use_nulls 1 --enable_join_runtime_filters 0 \
    --query "SELECT count() FROM ($query) AS g INNER JOIN (SELECT 0 AS val) AS k ON g.x30 = k.val"
