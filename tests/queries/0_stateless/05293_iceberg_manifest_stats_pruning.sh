#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row count of an Iceberg read under a filter, as MergeTree without
# column statistics reports it. A filter that drops at least one live data file gives the rows of
# the surviving files, imprecise; a filter that drops nothing gives unknown; a filter that drops
# every file gives a precise 0. The row policy counts as a filter. Delete files are not opened:
# rows before deletes, imprecise.
# T3 partition and min/max pruning, T3b every file pruned, T3c row policy, T3d partition pruning
# off, T4 position deletes, T4b deletes with a pruning filter. Every arm also runs with the gate off.
# The MergeTree twins (no column statistics) print the same labels and numbers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1"
ON="--use_iceberg_manifest_statistics=1"
OFF="--use_iceberg_manifest_statistics=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
TEST_USER="${CLICKHOUSE_DATABASE}_user"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# Prints the `Join:` and `ResultRows:` lines of the logical plan. Usage: labels <query> [client flags].
labels()
{
    local query="$1"
    shift
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${query})
        WHERE explain LIKE '%Join: %' OR explain LIKE '%ResultRows: %'"
}

# p and p_policy: 5 data files, per INSERT one per value of r. us: 10 + 5 rows, eu: 10 + 5, asia: 10.
# k is in [0, 29] in the first INSERT's files and in [100, 109] in the second's.
# The filters stay off the join keys: a filter on a join key is pushed to both sides.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE p (k Int32, r String, v Int64) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r);
    INSERT INTO p SELECT number, ['us', 'eu', 'asia'][number % 3 + 1], number FROM numbers(30);
    INSERT INTO p SELECT number, ['us', 'eu'][number % 2 + 1], number FROM numbers(100, 10);
    CREATE TABLE p_policy (k Int32, r String, v Int64) ENGINE = IcebergLocal('${LAKE}/p_policy') PARTITION BY (r);
    INSERT INTO p_policy SELECT number, ['us', 'eu', 'asia'][number % 3 + 1], number FROM numbers(30);
    INSERT INTO p_policy SELECT number, ['us', 'eu'][number % 2 + 1], number FROM numbers(100, 10);
    CREATE TABLE unp (k Int32, x Int32) ENGINE = IcebergLocal('${LAKE}/unp');
    INSERT INTO unp SELECT number, number % 1000 + 1 FROM numbers(10000);
    CREATE TABLE d (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/d') SETTINGS iceberg_format_version = 2;
    INSERT INTO d SELECT number, number FROM numbers(100);
    DELETE FROM d WHERE k < 20;
    CREATE TABLE d2 (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/d2') SETTINGS iceberg_format_version = 2;
    INSERT INTO d2 SELECT number, number FROM numbers(100);
    INSERT INTO d2 SELECT number, number FROM numbers(100, 100);
    DELETE FROM d2 WHERE k >= 190;
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
    CREATE TABLE twin_p (k Int32, r String, v Int64) ENGINE = MergeTree PARTITION BY r ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_p SELECT number, ['us', 'eu', 'asia'][number % 3 + 1], number FROM numbers(30);
    INSERT INTO twin_p SELECT number, ['us', 'eu'][number % 2 + 1], number FROM numbers(100, 10);
    CREATE TABLE twin_unp (k Int32, x Int32) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_unp SELECT number, number % 1000 + 1 FROM numbers(10000);
    CREATE TABLE twin_d (k Int32, v Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_d SELECT number, number FROM numbers(100);
    DELETE FROM twin_d WHERE k < 20;
    CREATE TABLE twin_d2 (k Int32, v Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_d2 SELECT number, number FROM numbers(100);
    INSERT INTO twin_d2 SELECT number, number FROM numbers(100, 100);
    DELETE FROM twin_d2 WHERE k >= 190;
"

echo '--- fixture: files and rows per Iceberg table and content'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, content, count(), sum(record_count), arraySort(groupArray(record_count))
    FROM system.iceberg_files WHERE database = currentDatabase()
    GROUP BY table, content ORDER BY table, content"

for FILTER in "p.r = 'us'" "p.k >= 100" "p.k >= 1000000"; do
    echo "--- T3 twin: WHERE ${FILTER}"
    labels "SELECT count() FROM mt AS m JOIN twin_p AS p ON m.x = p.v WHERE ${FILTER}"
    echo "--- T3 gate on: WHERE ${FILTER}"
    labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE ${FILTER}" ${ON}
    echo "--- T3 gate off: WHERE ${FILTER}"
    labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE ${FILTER}" ${OFF}
done

echo "--- T3 twin: unpartitioned, WHERE t.x = 5 drops no file"
labels "SELECT count() FROM mt AS m JOIN twin_unp AS t ON m.k = t.k WHERE t.x = 5"
echo "--- T3 gate on: unpartitioned, WHERE t.x = 5 drops no file"
labels "SELECT count() FROM mt AS m JOIN unp AS t ON m.k = t.k WHERE t.x = 5" ${ON}
echo "--- T3 gate off: unpartitioned, WHERE t.x = 5 drops no file"
labels "SELECT count() FROM mt AS m JOIN unp AS t ON m.k = t.k WHERE t.x = 5" ${OFF}

# T3c: the policy k >= 100 drops the first INSERT's 3 files by min/max, as the executed read does.
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH plaintext_password BY 'policy_pwd'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.p_policy FOR SELECT USING k >= 100 TO ${TEST_USER}"
AS_USER="--user ${TEST_USER} --password policy_pwd"
echo "--- T3c gate on: row policy k >= 100, no WHERE"
labels "SELECT count() FROM mt AS m JOIN p_policy AS p ON m.x = p.v" ${AS_USER} ${ON}
echo "--- T3c gate off: row policy k >= 100, no WHERE"
labels "SELECT count() FROM mt AS m JOIN p_policy AS p ON m.x = p.v" ${AS_USER} ${OFF}
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.p_policy"
${CLICKHOUSE_CLIENT} --query "DROP USER ${TEST_USER}"

# T3d: with partition pruning off the read drops no file either (min/max included).
echo "--- T3d gate on: use_iceberg_partition_pruning = 0, WHERE p.r = 'us'"
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.r = 'us'" --use_iceberg_partition_pruning=0 ${ON}
echo "--- T3d gate off: use_iceberg_partition_pruning = 0, WHERE p.r = 'us'"
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.r = 'us'" --use_iceberg_partition_pruning=0 ${OFF}

echo '--- T4 twin: 100 rows, 20 deleted'
labels "SELECT count() FROM mt AS m JOIN twin_d AS b ON m.k = b.k"
echo '--- T4 gate on: 100 rows, 20 deleted by a position delete file'
labels "SELECT count() FROM mt AS m JOIN d AS b ON m.k = b.k" ${ON}
echo '--- T4 gate off'
labels "SELECT count() FROM mt AS m JOIN d AS b ON m.k = b.k" ${OFF}

# T4b: the filter drops the first file; the surviving file has 100 rows, 10 of them deleted.
echo '--- T4b twin: WHERE b.k >= 100'
labels "SELECT count() FROM mt AS m JOIN twin_d2 AS b ON m.x = b.v WHERE b.k >= 100"
echo '--- T4b gate on: WHERE b.k >= 100'
labels "SELECT count() FROM mt AS m JOIN d2 AS b ON m.x = b.v WHERE b.k >= 100" ${ON}
echo '--- T4b gate off: WHERE b.k >= 100'
labels "SELECT count() FROM mt AS m JOIN d2 AS b ON m.x = b.v WHERE b.k >= 100" ${OFF}

rm -rf "${LAKE}"
