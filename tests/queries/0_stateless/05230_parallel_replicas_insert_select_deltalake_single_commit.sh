#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# Regression test for issue #120714: an INSERT SELECT into a Delta Lake table was distributed
# across replicas, so every replica opened its own Delta transaction and committed independently.
# The commits raced for the same log version, the INSERT failed with DELTA_KERNEL_ERROR, and the
# table was left with a spurious extra commit (or only part of the rows). One INSERT owes exactly
# one commit, so the write must run on the initiator alone while reading stays distributed.
#
# The second part asserts the same invariant for the other way the whole INSERT gets distributed:
# a cluster table function as the source, which forwards it to every node of that cluster.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_pr_single_commit"
# The database name makes the marker unique per run, so repeated runs do not read each other's rows.
LOG_COMMENT="dl-single-commit-${CLICKHOUSE_DATABASE}"

rm -rf "$TABLE_PATH"

count_commits() {
    ls -1 "$TABLE_PATH"/_delta_log/*.json 2>/dev/null | wc -l | tr -d ' '
}

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS pr_src;
CREATE TABLE pr_src (id Int64, s String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000;
SYSTEM STOP MERGES pr_src;
INSERT INTO pr_src SELECT number, toString(number) FROM numbers(10000);
INSERT INTO pr_src SELECT number, toString(number) FROM numbers(10000, 10000);
INSERT INTO pr_src SELECT number, toString(number) FROM numbers(20000, 10000);

SET allow_delta_lake_writes = 1, allow_delta_lake_create_table = 1;
DROP TABLE IF EXISTS pr_dl;
CREATE TABLE pr_dl (id Int64, s String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"

echo "commits after create: $(count_commits)"

INSERT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_insert.err"
if $CLICKHOUSE_CLIENT --query "
SET allow_delta_lake_writes = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_distributed_insert_select = 2,
    automatic_parallel_replicas_mode = 0,
    log_comment = '${LOG_COMMENT}';
INSERT INTO pr_dl SELECT id, s FROM pr_src;
" > /dev/null 2> "$INSERT_ERR"; then
    echo "insert: ok"
else
    echo "insert: failed"
    head -c 400 "$INSERT_ERR"
fi
rm -f "$INSERT_ERR"

# Exactly one new commit on top of the CREATE TABLE commit. This oracle keeps working if the Delta
# kernel ever starts retrying conflicts, where the symptom would be extra commits and no error.
echo "commits after insert: $(count_commits)"

$CLICKHOUSE_CLIENT --query "
SET allow_delta_lake_writes = 1;
SELECT 'rows', count() FROM pr_dl;

SYSTEM FLUSH LOGS query_log;
-- One sink, on the initiator. 'QueryStart' counts every execution whether it committed or threw.
SELECT 'insert executions', count() FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '${LOG_COMMENT}'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- Without this the four oracles above also pass when parallel replicas are disabled outright.
SELECT 'parallel read', maxIf(ProfileEvents['ParallelReplicasUsedCount'] > 0, is_initial_query)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '${LOG_COMMENT}'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

DROP TABLE pr_dl;
DROP TABLE pr_src;
"

rm -rf "$TABLE_PATH"


CLUSTER_TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_pr_cluster_source"
CLUSTER_SRC_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_src"
CLUSTER_LOG_COMMENT="dl-cluster-source-${CLICKHOUSE_DATABASE}"

rm -rf "$CLUSTER_TABLE_PATH" "$CLUSTER_SRC_DIR"
mkdir -p "$CLUSTER_SRC_DIR"
# Three files, so the read has something to spread over the three nodes of the cluster.
for part in 0 1 2; do
    $CLICKHOUSE_CLIENT --query \
        "SELECT number, toString(number) FROM numbers($((part * 10000)), 10000) FORMAT CSV" \
        > "${CLUSTER_SRC_DIR}/part${part}.csv"
done

count_cluster_commits() {
    ls -1 "$CLUSTER_TABLE_PATH"/_delta_log/*.json 2>/dev/null | wc -l | tr -d ' '
}

$CLICKHOUSE_CLIENT --query "
SET allow_delta_lake_writes = 1, allow_delta_lake_create_table = 1;
DROP TABLE IF EXISTS pr_dl_cluster;
CREATE TABLE pr_dl_cluster (id Int64, s String) ENGINE = DeltaLakeLocal('${CLUSTER_TABLE_PATH}', Parquet);
"

echo "cluster source: commits after create: $(count_cluster_commits)"

INSERT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_cluster_insert.err"
if $CLICKHOUSE_CLIENT --query "
INSERT INTO pr_dl_cluster SELECT c1, c2 FROM fileCluster(
    'test_cluster_one_shard_three_replicas_localhost',
    '${CLICKHOUSE_TEST_UNIQUE_NAME}_src/part*.csv', 'CSV', 'c1 Int64, c2 String')
SETTINGS parallel_distributed_insert_select = 2, allow_delta_lake_writes = 1,
    log_comment = '${CLUSTER_LOG_COMMENT}';
" > /dev/null 2> "$INSERT_ERR"; then
    echo "cluster source: insert: ok"
else
    echo "cluster source: insert: failed"
    head -c 400 "$INSERT_ERR"
fi
rm -f "$INSERT_ERR"

echo "cluster source: commits after insert: $(count_cluster_commits)"

$CLICKHOUSE_CLIENT --query "
SET allow_delta_lake_writes = 1;
SELECT 'cluster source: rows', count() FROM pr_dl_cluster;

SYSTEM FLUSH LOGS query_log;
SELECT 'cluster source: insert executions', count() FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '${CLUSTER_LOG_COMMENT}'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- A fallback still reads the source on every node of its cluster, from the initiator. Without this
-- the oracles above also pass when the source has degraded to a single local reader.
SELECT 'cluster source: distributed read', maxIf(ProfileEvents['Shards'] > 1, is_initial_query)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '${CLUSTER_LOG_COMMENT}'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

DROP TABLE pr_dl_cluster;
"

rm -rf "$CLUSTER_TABLE_PATH" "$CLUSTER_SRC_DIR"
