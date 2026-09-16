#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: needs MinIO and a Replicated table

# https://github.com/ClickHouse/ClickHouse/issues/120485
#
# `INSERT INTO <replicated table> SELECT * FROM url(...)` (or `s3(...)`) with parallel replicas enabled:
# `parallel_replicas_for_cluster_engines` converts the plain table function into a cluster storage on the
# initiator, so the INSERT takes the distributed `parallel_distributed_insert_select` path and forwards the
# query to every replica of the cluster. The forwarded query text still named the plain function, and a replica
# running it as a secondary query created a plain storage that expanded the globs and read every file on its
# own instead of taking its share of the read tasks from the initiator, so N replicas inserted the data N times.
# The forwarded query must name the `*Cluster` variant, the same way the SELECT path does.
#
# The three "replicas" of `test_cluster_one_shard_three_replicas_localhost` are all this server, so the
# secondary queries are visible in the local query log.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05218"

# Three files of different sizes: 10 + 20 + 30 = 60 rows, so a duplicated file changes the count.
for i in 1 2 3
do
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/part_${i}.tsv', 'TSV', 'x UInt32') SELECT number FROM numbers(${i} * 10)"
done

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dst_05218 SYNC;
    CREATE TABLE dst_05218 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_05218', 'r1') ORDER BY x;
"

SETTINGS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_cluster_engines = 1, parallel_distributed_insert_select = 2, log_queries = 1"

QUERY_ID_URL="05218_${CLICKHOUSE_DATABASE}_url"
QUERY_ID_S3="05218_${CLICKHOUSE_DATABASE}_s3"

echo "--- url ---"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_URL}" -q "
    INSERT INTO dst_05218 SELECT * FROM url('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM dst_05218"

echo "--- s3 ---"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE dst_05218"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_S3}" -q "
    INSERT INTO dst_05218 SELECT * FROM s3('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM dst_05218"

# The INSERT must really have been distributed: every replica ran the forwarded INSERT, the forwarded
# query names the `*Cluster` function, and the replicas together read every file exactly once.
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
            count() AS replicas,
            countIf(query LIKE '%Cluster(%') AS cluster_function_queries,
            sum(read_rows) AS rows_read_by_replicas
        FROM system.query_log
        WHERE initial_query_id IN (SELECT query_id FROM initial)
            AND is_initial_query = 0
            AND query_kind = 'Insert'
            AND type = 'QueryFinish'
            AND event_date >= yesterday()"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE dst_05218 SYNC"
