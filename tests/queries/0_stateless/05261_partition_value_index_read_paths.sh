#!/usr/bin/env bash
# Tags: no-ordinary-database
# no-ordinary-database: uses transactions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The partition value index on read paths that do not read the shared parts snapshot as is (projections,
# parallel replicas, transactions) or that are sensitive to the set of parts (FINAL), and with new partitions
# appearing and partitions being dropped concurrently. The results with and without the index must be equal.

WITH_INDEX="--use_partition_value_index 1 --partition_value_index_min_parts 0"
WITHOUT_INDEX="--use_partition_value_index 0"

function compare()
{
    local name=$1
    local query=$2
    shift 2

    local with_index
    local without_index
    # shellcheck disable=SC2086
    with_index=$($CLICKHOUSE_CLIENT $WITH_INDEX "$@" -q "$query")
    # shellcheck disable=SC2086
    without_index=$($CLICKHOUSE_CLIENT $WITHOUT_INDEX "$@" -q "$query")

    if [ "$with_index" == "$without_index" ]
    then
        echo "$name: $with_index"
    else
        echo "$name: results differ: '$with_index' with the index, '$without_index' without the index"
    fi
}

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pvi_projection;
    DROP TABLE IF EXISTS t_pvi_final;
    DROP TABLE IF EXISTS t_pvi_concurrent;

    CREATE TABLE t_pvi_projection (tenant UInt16, k UInt64, v UInt64, PROJECTION by_k (SELECT * ORDER BY k))
    ENGINE = MergeTree PARTITION BY tenant ORDER BY v;

    CREATE TABLE t_pvi_final (tenant UInt16, k UInt64, v UInt64)
    ENGINE = ReplacingMergeTree(v) PARTITION BY tenant ORDER BY k;

    CREATE TABLE t_pvi_concurrent (tenant UInt16, v UInt64)
    ENGINE = MergeTree PARTITION BY tenant ORDER BY v;

    SYSTEM STOP MERGES t_pvi_final;"

$CLICKHOUSE_CLIENT --max_partitions_per_insert_block 0 -q "
    INSERT INTO t_pvi_projection SELECT number % 20, number % 1000, number FROM numbers(2000);

    INSERT INTO t_pvi_final SELECT number % 20, number % 100, number FROM numbers(1000);
    INSERT INTO t_pvi_final SELECT number % 20, number % 100, number + 1 FROM numbers(1000);

    INSERT INTO t_pvi_concurrent SELECT number % 10, number FROM numbers(1000);"

compare "projection" "SELECT count(), sum(v) FROM t_pvi_projection WHERE tenant = 7 AND k = 107" --optimize_use_projections 1
compare "projection, set" "SELECT count(), sum(v) FROM t_pvi_projection WHERE tenant IN (7, 8, 19) AND k BETWEEN 100 AND 200" --optimize_use_projections 1

compare "final" "SELECT count(), sum(v) FROM t_pvi_final FINAL WHERE tenant = 7"
compare "final, range" "SELECT count(), sum(v) FROM t_pvi_final FINAL WHERE tenant >= 15"

compare "parallel replicas" "SELECT count(), sum(v) FROM t_pvi_projection WHERE tenant IN (7, 8, 19)" \
    --enable_parallel_replicas 1 --parallel_replicas_for_non_replicated_merge_tree 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas parallel_replicas --parallel_replicas_local_plan 1

# A plain `MergeTree` table, so that the result does not depend on `optimize_on_insert` (randomized in CI).
compare "transaction" "BEGIN TRANSACTION; SELECT count(), sum(v) FROM t_pvi_concurrent WHERE tenant = 7; COMMIT;"

# New partitions appear and partitions are dropped concurrently with the queries. The concurrent changes affect
# only the tenants from 100, so the result of the queries for tenant 7 does not change.
function change_partitions()
{
    local i=0
    while [ "$(date +%s)" -lt "$1" ]
    do
        $CLICKHOUSE_CLIENT -q "INSERT INTO t_pvi_concurrent SELECT 100 + $i % 20, number FROM numbers(10)"
        $CLICKHOUSE_CLIENT -q "ALTER TABLE t_pvi_concurrent DROP PARTITION $((100 + (i + 10) % 20))"
        i=$((i + 1))
    done
}

# shellcheck disable=SC2086
expected=$($CLICKHOUSE_CLIENT $WITHOUT_INDEX -q "SELECT count(), sum(v) FROM t_pvi_concurrent WHERE tenant = 7")

end_time=$(( $(date +%s) + 5 ))
change_partitions "$end_time" &

mismatches=0
while [ "$(date +%s)" -lt "$end_time" ]
do
    # shellcheck disable=SC2086
    result=$($CLICKHOUSE_CLIENT $WITH_INDEX -q "SELECT count(), sum(v) FROM t_pvi_concurrent WHERE tenant = 7")
    if [ "$result" != "$expected" ]
    then
        mismatches=$((mismatches + 1))
        echo "concurrent: unexpected result '$result', expected '$expected'"
    fi
done

wait

echo "concurrent: $mismatches mismatches"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_pvi_projection;
    DROP TABLE t_pvi_final;
    DROP TABLE t_pvi_concurrent;"
