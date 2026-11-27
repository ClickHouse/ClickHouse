#!/usr/bin/env bash
# A filter over a chain of `x + x` aliases is pushed down to the `LEFT JOIN` as one DAG with shared nodes.
# Checking whether that filter is always false must visit each node once, not every path.
# The queries are built here and not kept in a .sql file, because the AST fuzzer takes its seeds only from
# .sql files and its mutations of this chain run into issue #123724 (names that grow exponentially).
# Random settings limits: query_plan_optimize_join_order_limit=(1, None)

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

chain() {
    local join=$1 query
    query="SELECT $(projection 'r.val' 1) FROM (SELECT number AS id FROM numbers(2)) AS l"
    query="$query $join (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r ON l.id = r.id"
    for i in $(seq 2 $LEVELS); do
        query="SELECT $(projection "a$((i - 1))_$STEPS" "$i") FROM ($query)"
    done
    echo "$query"
}

# JIT compilation names an expression by its full text, which grows exponentially here (issue #123724),
# and so does planning the first query with `join_use_nulls = 1`.
settings="SETTINGS join_use_nulls = 0, compile_expressions = 0, ast_fuzzer_runs = 0, collect_hash_table_stats_during_joins = 1"

query_id="${CLICKHOUSE_DATABASE}_partial_result_shared_nodes_join_filter_$RANDOM$RANDOM"
join_query="SELECT count() FROM ($(chain 'LEFT JOIN')) AS g INNER JOIN (SELECT 0 AS val) AS k ON g.a${LEVELS}_${STEPS} >= k.val $settings"
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "$join_query"

# Checking whether an `ANY LEFT JOIN` under such a filter can be converted to a `SEMI` join walks the same DAG.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ($(chain 'ANY LEFT JOIN')) WHERE a${LEVELS}_${STEPS} >= 0 $settings"

# The first query has to reach the statistics cache, or it would not hash the filter at all. It logs nothing when a
# concurrent run of this test stores the same entry while it runs, so it runs once more, when the entry is cached.
${CLICKHOUSE_CLIENT} --query_id="${query_id}_cached" --query "$join_query"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --param_first="$query_id" --param_second="${query_id}_cached" --query "SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND query_id IN ({first:String}, {second:String}) AND logger_name = 'HashTablesStatistics'"
