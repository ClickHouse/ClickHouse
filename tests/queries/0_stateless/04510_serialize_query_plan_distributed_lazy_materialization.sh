#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base_settings="enable_analyzer=1, serialize_query_plan=1, query_plan_optimize_lazy_materialization=1, distributed_push_down_limit=1, skip_unavailable_shards=0, explain_query_plan_default='legacy'"
query="SELECT payload FROM dlm_dist ORDER BY key DESC, tie ASC LIMIT 3 OFFSET 2"

explain_query()
{
    local select_query=$1
    local extra_settings=$2
    local settings=$base_settings
    if [[ -n $extra_settings ]]
    then
        settings+=", ${extra_settings}"
    fi
    "$CLICKHOUSE_CLIENT" --query "EXPLAIN actions=1, distributed=1 ${select_query} SETTINGS ${settings}"
}

expect_enabled()
{
    local output
    output=$(explain_query "$query" "query_plan_optimize_distributed_lazy_materialization=1")

    if [[ $(grep -c "Distributed TopK candidate limit" <<< "$output") -ne 2 ]] \
        || [[ $(grep -c "Limit 5" <<< "$output") -lt 2 ]] \
        || [[ $(grep -c "Sorting" <<< "$output") -lt 3 ]]
    then
        echo "eligible: unexpected plan"
        echo "$output"
        exit 1
    fi

    echo "eligible: enabled"
}

expect_disabled()
{
    local name=$1
    local select_query=$2
    local extra_settings=$3
    local output
    output=$(explain_query "$select_query" "$extra_settings")

    if grep -q "Distributed TopK candidate limit" <<< "$output"
    then
        echo "${name}: unexpectedly enabled"
        echo "$output"
        exit 1
    fi

    echo "${name}: disabled"
}

expect_unbounded_coordination_limit()
{
    local output
    output=$(explain_query \
        "SELECT payload FROM dlm_dist ORDER BY key DESC, tie ASC LIMIT 1000001" \
        "query_plan_optimize_distributed_lazy_materialization=1, query_plan_max_limit_for_lazy_materialization=0")

    if ! grep -q "Distributed TopK candidate limit" <<< "$output"
    then
        echo "coordination row limit: unexpectedly disabled"
        echo "$output"
        exit 1
    fi

    echo "coordination row limit: enabled"
}

"$CLICKHOUSE_CLIENT" --multiquery --query "
    DROP TABLE IF EXISTS dlm_dist;
    DROP TABLE IF EXISTS dlm_local;
    CREATE TABLE dlm_local (key UInt64, tie Nullable(UInt64), payload String) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE dlm_dist AS dlm_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dlm_local);
"

expect_disabled "default setting" "$query" ""
expect_enabled
expect_disabled "serialized plan" "$query" "query_plan_optimize_distributed_lazy_materialization=1, serialize_query_plan=0"
expect_disabled "lazy materialization" "$query" "query_plan_optimize_distributed_lazy_materialization=1, query_plan_optimize_lazy_materialization=0"
expect_disabled "limit pushdown" "$query" "query_plan_optimize_distributed_lazy_materialization=1, distributed_push_down_limit=0"
expect_disabled "unavailable shards" "$query" "query_plan_optimize_distributed_lazy_materialization=1, skip_unavailable_shards=1"
expect_disabled "zero limit" "SELECT payload FROM dlm_dist ORDER BY key DESC, tie ASC LIMIT 0" "query_plan_optimize_distributed_lazy_materialization=1"
expect_disabled "with ties" "SELECT payload FROM dlm_dist ORDER BY key DESC, tie ASC LIMIT 3 WITH TIES" "query_plan_optimize_distributed_lazy_materialization=1"
expect_disabled "read till end" "$query" "query_plan_optimize_distributed_lazy_materialization=1, exact_rows_before_limit=1"
expect_disabled "overflow" "SELECT payload FROM dlm_dist ORDER BY key DESC, tie ASC LIMIT 18446744073709551615 OFFSET 1" "query_plan_optimize_distributed_lazy_materialization=1, query_plan_max_limit_for_lazy_materialization=0"
expect_unbounded_coordination_limit
expect_disabled "maximum limit" "$query" "query_plan_optimize_distributed_lazy_materialization=1, query_plan_max_limit_for_lazy_materialization=4"
expect_disabled "parallel replicas" "$query" "query_plan_optimize_distributed_lazy_materialization=1, allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=2"

"$CLICKHOUSE_CLIENT" --multiquery --query "
    CREATE DATABASE IF NOT EXISTS shard_0;
    CREATE DATABASE IF NOT EXISTS shard_1;
    DROP TABLE IF EXISTS shard_0.dlm_local_04510;
    DROP TABLE IF EXISTS shard_1.dlm_local_04510;
    DROP TABLE IF EXISTS dlm_interleaved_dist;
    CREATE TABLE shard_0.dlm_local_04510 (key UInt64, tie UInt64, payload String) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE shard_1.dlm_local_04510 (key UInt64, tie UInt64, payload String) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE dlm_interleaved_dist (key UInt64, tie UInt64, payload String)
        ENGINE = Distributed(test_cluster_two_shards_different_databases, '', dlm_local_04510);
    INSERT INTO shard_0.dlm_local_04510 VALUES (9, 2, 's0-9'), (7, 1, 's0-7'), (5, 3, 's0-5');
    INSERT INTO shard_1.dlm_local_04510 VALUES (10, 4, 's1-10'), (8, 0, 's1-8'), (6, 2, 's1-6');
"

winner_query="SELECT payload FROM dlm_interleaved_dist ORDER BY key DESC, tie ASC LIMIT 3 OFFSET 2"
enabled=$("$CLICKHOUSE_CLIENT" --query "${winner_query} SETTINGS ${base_settings}, query_plan_optimize_distributed_lazy_materialization=1")
disabled=$("$CLICKHOUSE_CLIENT" --query "${winner_query} SETTINGS ${base_settings}, query_plan_optimize_distributed_lazy_materialization=0")
expected=$'s1-8\ns0-7\ns1-6'
if [[ "$enabled" != "$expected" ]]
then
    echo "interleaved winners: unexpected enabled result"
    printf '%s\n' "$enabled"
    exit 1
fi
if [[ "$disabled" != "$enabled" ]]
then
    echo "interleaved winners: enabled and disabled differ"
    printf 'enabled:\n%s\ndisabled:\n%s\n' "$enabled" "$disabled"
    exit 1
fi
echo "interleaved winners: correct"

"$CLICKHOUSE_CLIENT" --multiquery --query "
    DROP TABLE dlm_interleaved_dist;
    DROP TABLE shard_0.dlm_local_04510;
    DROP TABLE shard_1.dlm_local_04510;
    DROP TABLE dlm_dist;
    DROP TABLE dlm_local;
"
