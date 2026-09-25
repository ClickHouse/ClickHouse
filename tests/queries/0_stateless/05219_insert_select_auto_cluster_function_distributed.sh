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
    DROP TABLE IF EXISTS dist_05219_sharded;
    DROP TABLE IF EXISTS local_05219_sharded;
    CREATE TABLE local_05219 (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist_05219 AS local_05219
        ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05219, rand());
    CREATE TABLE local_05219_sharded (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist_05219_sharded AS local_05219_sharded
        ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05219_sharded, x);
"

SETTINGS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_cluster_engines = 1, parallel_distributed_insert_select = 2, log_queries = 1"

# The query ids and the query log lookups below must isolate this run: a re-run on the same server must
# not pick up the secondary queries of a previous run.
QUERY_ID_SUFFIX="${CLICKHOUSE_DATABASE}_$(date +%s%N)_${RANDOM}"
QUERY_ID_URL="05219_url_${QUERY_ID_SUFFIX}"
QUERY_ID_S3="05219_s3_${QUERY_ID_SUFFIX}"

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
for pair in "${QUERY_ID_URL} url" "${QUERY_ID_S3} s3"
do
    query_id="${pair%% *}"
    echo "--- forwarded queries of ${pair##* } ---"
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
                AND event_time >= now() - INTERVAL 10 MINUTE
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
            AND event_date >= yesterday()
            AND event_time >= now() - INTERVAL 10 MINUTE"
done

# A deterministic sharding key states where every row must live, and a cluster table function hands out
# files by hashing their paths, so the rows a shard reads cannot satisfy it. With
# `parallel_distributed_insert_select = 2` - which inserts into the shard's own local table - the
# distributed execution is therefore skipped and the ordinary INSERT SELECT places the rows through the
# `Distributed` sink. With `= 1` the forwarded INSERT still targets the `Distributed` table, so every shard
# re-shards its own rows and the distributed execution is kept.
SHARDED_SETTINGS="enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_cluster_engines = 1, distributed_foreground_insert = 1, log_queries = 1"

QUERY_ID_SHARDED_2="05219_sharded2_${QUERY_ID_SUFFIX}"
QUERY_ID_SHARDED_1="05219_sharded1_${QUERY_ID_SUFFIX}"

echo "--- deterministic sharding key, parallel_distributed_insert_select = 2 ---"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_SHARDED_2}" -q "
    INSERT INTO dist_05219_sharded SELECT * FROM url('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32')
    SETTINGS ${SHARDED_SETTINGS}, parallel_distributed_insert_select = 2"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM local_05219_sharded"

echo "--- deterministic sharding key, parallel_distributed_insert_select = 1 ---"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE local_05219_sharded"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_SHARDED_1}" -q "
    INSERT INTO dist_05219_sharded SELECT * FROM url('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32')
    SETTINGS ${SHARDED_SETTINGS}, parallel_distributed_insert_select = 1"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM local_05219_sharded"

# Only the number of forwarded queries that name the `*Cluster` function is asserted here: whether the
# `Distributed` sink's own per-shard inserts reach the query log is not what these cases are about.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
for pair in "${QUERY_ID_SHARDED_2} 2" "${QUERY_ID_SHARDED_1} 1"
do
    query_id="${pair%% *}"
    echo "--- forwarded cluster-function queries with parallel_distributed_insert_select = ${pair##* } ---"
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
                AND event_time >= now() - INTERVAL 10 MINUTE
        )
        SELECT countIf(query LIKE '%Cluster(''test_cluster_two_shards_localhost''%') AS cluster_function_queries
        FROM system.query_log
        WHERE initial_query_id IN (SELECT query_id FROM initial)
            AND is_initial_query = 0
            AND query_kind = 'Insert'
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
            AND event_time >= now() - INTERVAL 10 MINUTE"
done

# A `dictGet` sharding key is not deterministic across queries - the dictionary can be reloaded - but it is
# fixed within one and states where a row belongs, and `allow_nondeterministic_optimize_skip_unused_shards`
# lets reads prune by it. It must be treated like any other placement-preserving key.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dict_source_05219 (key UInt32, shard UInt64) ENGINE = Memory;
    INSERT INTO dict_source_05219 SELECT number, number % 2 FROM numbers(100);
    CREATE DICTIONARY dict_05219 (key UInt32, shard UInt64)
        PRIMARY KEY key
        SOURCE(CLICKHOUSE(host '127.0.0.1' port tcpPort() table 'dict_source_05219' db currentDatabase() user 'default'))
        LIFETIME(0) LAYOUT(HASHED());
    SYSTEM RELOAD DICTIONARY dict_05219;
    CREATE TABLE local_05219_dict (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist_05219_dict AS local_05219_dict
        ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_05219_dict,
                             dictGetUInt64('${CLICKHOUSE_DATABASE}.dict_05219', 'shard', x));
"

QUERY_ID_DICT="05219_dict_${QUERY_ID_SUFFIX}"

echo "--- dictGet sharding key, parallel_distributed_insert_select = 2 ---"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_DICT}" -q "
    INSERT INTO dist_05219_dict SELECT * FROM url('${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32')
    SETTINGS ${SHARDED_SETTINGS}, parallel_distributed_insert_select = 2"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM local_05219_dict"

# An explicitly written `*Cluster` source names a cluster of its own. The shards that run the forwarded
# query are those of the `Distributed` table, and they reject a cluster name their own `remote_servers` does
# not define, so the forwarded query must name the destination's cluster and not the source's.
QUERY_ID_EXPLICIT="05219_explicit_${QUERY_ID_SUFFIX}"

echo "--- explicitly written s3Cluster with a different cluster ---"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE local_05219"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID_EXPLICIT}" -q "
    INSERT INTO dist_05219
    SELECT * FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', '${S3_DIR}/part_{1..3}.tsv', 'TSV', 'x UInt32')
    SETTINGS parallel_distributed_insert_select = 2, log_queries = 1"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(x) FROM local_05219"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
for pair in "${QUERY_ID_DICT} dict" "${QUERY_ID_EXPLICIT} explicit"
do
    query_id="${pair%% *}"
    echo "--- forwarded cluster-function queries of ${pair##* } ---"
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
                AND event_time >= now() - INTERVAL 10 MINUTE
        )
        SELECT
            countIf(query LIKE '%Cluster(''test_cluster_two_shards_localhost''%') AS to_destination_cluster,
            countIf(query LIKE '%Cluster(''test_cluster_one_shard_three_replicas_localhost''%') AS to_source_cluster
        FROM system.query_log
        WHERE initial_query_id IN (SELECT query_id FROM initial)
            AND is_initial_query = 0
            AND query_kind = 'Insert'
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
            AND event_time >= now() - INTERVAL 10 MINUTE"
done

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dist_05219;
    DROP TABLE local_05219;
    DROP TABLE dist_05219_sharded;
    DROP TABLE local_05219_sharded;
    DROP TABLE dist_05219_dict;
    DROP TABLE local_05219_dict;
    DROP DICTIONARY dict_05219;
    DROP TABLE dict_source_05219;
"
