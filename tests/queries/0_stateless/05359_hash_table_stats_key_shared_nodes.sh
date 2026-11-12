#!/usr/bin/env bash
# A filter over a chain of `x + x` aliases below a keyed `GROUP BY` is one DAG with shared nodes.
# The hash table statistics key of the aggregation must hash it once per node, not once per path.

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
    local query
    query="SELECT l.id AS k, $(projection 'r.val' 1) FROM (SELECT number AS id FROM numbers(2)) AS l"
    query="$query LEFT JOIN (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r ON l.id = r.id"
    for i in $(seq 2 $LEVELS); do
        query="SELECT k, $(projection "a$((i - 1))_$STEPS" "$i") FROM ($query)"
    done
    echo "$query"
}

# `join_use_nulls` and `compile_expressions` reach other walks that are exponential here, see 05359_partial_result_shared_nodes_join_filter.
settings="SETTINGS join_use_nulls = 0, compile_expressions = 0, ast_fuzzer_runs = 0, collect_hash_table_stats_during_joins = 0, collect_hash_table_stats_during_aggregation = 1"

query_id="${CLICKHOUSE_DATABASE}_hash_table_stats_key_shared_nodes_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "SELECT k, count() FROM ($(chain)) WHERE a${LEVELS}_${STEPS} >= 0 GROUP BY k ORDER BY k $settings"

# The query has to reach the statistics cache, or it would not hash the filter at all.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --param_query_id="$query_id" --query "SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = {query_id:String} AND logger_name = 'HashTablesStatistics'"
