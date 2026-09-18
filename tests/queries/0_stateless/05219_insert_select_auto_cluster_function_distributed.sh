#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# https://github.com/ClickHouse/ClickHouse/issues/120485
#
# The same defect as `05218_insert_select_auto_cluster_function_parallel_replicas`, but for a `Distributed`
# destination: `parallel_replicas_for_cluster_engines` converts the plain `url` / `s3` table function into a
# cluster storage on the initiator, so `INSERT INTO <Distributed table> SELECT * FROM url(...)` takes the
# `StorageDistributed::distributedWriteFromClusterStorage` path and forwards the query to every shard of the
# `Distributed` table's cluster. The forwarded query text still named the plain function, and a shard running
# it as a secondary query created a plain storage that expanded the globs and read every file on its own
# instead of taking its share of the read tasks from the initiator, so N shards inserted the data N times.
#
# The cluster named in the rewritten `*Cluster` function must be the cluster of the `Distributed` table,
# because its shards are the ones that run the forwarded query. The source cluster below
# (`cluster_for_parallel_replicas`) is deliberately a different one, so a rewrite that injected the source
# cluster would be visible in the forwarded query text.
#
# Both shards of `test_cluster_two_shards_localhost` are this server, so the secondary queries are visible in
# the local query log and a duplicated INSERT doubles the row count of the local table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05219"

# Three files of different sizes: 10 + 20 + 30 = 60 rows, so a duplicated file changes the count.
for i in 1 2 3
do
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/part_${i}.tsv', 'TSV', 'x UInt32') SELECT number FROM numbers(${i} * 10)"
done

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dist_05219;
    DROP TABLE IF EXISTS local_05219;
    CREATE TABLE local_05219 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist_05219 AS local_05219
        ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05219, rand());
"

SETTINGS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_cluster_engines = 1, parallel_distributed_insert_select = 2, log_queries = 1"

QUERY_ID_URL="05219_${CLICKHOUSE_DATABASE}_url"
QUERY_ID_S3="05219_${CLICKHOUSE_DATABASE}_s3"

echo "--- url ---"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_URL}" -q "
    INSERT INTO dist_05219 SELECT * FROM url('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM local_05219"

echo "--- s3 ---"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE local_05219"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_S3}" -q "
    INSERT INTO dist_05219 SELECT * FROM s3('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM local_05219"

# The INSERT must really have been distributed: every shard ran the forwarded INSERT, the forwarded query
# names the `*Cluster` function with the cluster of the `Distributed` table, and the shards together read
# every file exactly once. The secondary INSERTs run as the `default` user, so their `current_database` is
# not the test database - match them by `initial_query_id` instead.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
for query_id in "${QUERY_ID_URL}" "${QUERY_ID_S3}"
do
    echo "--- forwarded queries of ${query_id##*_} ---"
    $CLICKHOUSE_CLIENT -q "
        WITH initial AS
        (
            SELECT query_id
            FROM system.query_log
            WHERE current_database = currentDatabase()
                AND query_id = '${query_id}'
                AND is_initial_query = 1
                AND type = 'QueryFinish'
                AND event_date >= yesterday()
        )
        SELECT
            count() AS shards,
            countIf(query LIKE '%Cluster(''test_cluster_two_shards_localhost''%') AS cluster_function_queries,
            sum(read_rows) AS rows_read_by_shards
        FROM system.query_log
        WHERE initial_query_id IN (SELECT query_id FROM initial)
            AND is_initial_query = 0
            AND query_kind = 'Insert'
            AND type = 'QueryFinish'
            AND event_date >= yesterday()"
done

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dist_05219;
    DROP TABLE local_05219;
"
