-- Tags: distributed

-- Test for https://github.com/ClickHouse/ClickHouse/issues/114174
-- optimize_functions_to_subcolumns never applied through a Distributed table: the initiator
-- declines it (StorageDistributed::supportsOptimizationToSubcolumns is false) and shards used
-- to skip all optimization passes. Now shards apply the rewrite inside WHERE/PREWHERE, where
-- it cannot change the header expected by the initiator, so e.g. map['key'] reads a single
-- key subcolumn instead of the whole Map.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET enable_parallel_replicas = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS t_map_shard;
DROP TABLE IF EXISTS t_map_shard_dist;

CREATE TABLE t_map_shard (id UInt64, tags Map(String, String), arr Array(String), n Nullable(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

INSERT INTO t_map_shard SELECT
    number,
    mapFromArrays(arrayMap(i -> 'k' || toString(i), range(20)), arrayMap(i -> concat(repeat('v', 200), toString(number % 7)), range(20))),
    arrayMap(i -> repeat('a', 100), range(10)),
    if(number % 3 = 0, NULL, number)
FROM numbers(10000);

CREATE TABLE t_map_shard_dist AS t_map_shard ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_map_shard);

SELECT 'correctness';

SELECT count() FROM t_map_shard WHERE tags['k1'] != '' SETTINGS log_comment = '04838_map_local';
SELECT count() FROM t_map_shard_dist WHERE tags['k1'] != '' SETTINGS log_comment = '04838_map_dist_local', prefer_localhost_replica = 1, serialize_query_plan = 0;
SELECT count() FROM t_map_shard_dist WHERE tags['k1'] != '' SETTINGS log_comment = '04838_map_dist_remote', prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT count() FROM t_map_shard_dist WHERE tags['k1'] != '' SETTINGS log_comment = '04838_map_dist_plan', prefer_localhost_replica = 1, serialize_query_plan = 1;
SELECT count() FROM t_map_shard_dist WHERE tags['k1'] != '' SETTINGS log_comment = '04838_map_dist_noopt', prefer_localhost_replica = 1, serialize_query_plan = 0, optimize_functions_to_subcolumns = 0;

SELECT count() FROM t_map_shard_dist WHERE mapContainsKey(tags, 'k1');
SELECT count() FROM t_map_shard_dist WHERE mapContainsKey(tags, 'no_such_key');
SELECT count() FROM t_map_shard_dist WHERE n IS NULL;
SELECT count() FROM t_map_shard_dist WHERE notEmpty(arr) SETTINGS log_comment = '04838_arr_dist', prefer_localhost_replica = 1, serialize_query_plan = 0;
SELECT count() FROM t_map_shard_dist WHERE notEmpty(arr) SETTINGS log_comment = '04838_arr_dist_noopt', prefer_localhost_replica = 1, serialize_query_plan = 0, optimize_functions_to_subcolumns = 0;

SELECT 'header stability';

-- The same map key in SELECT / GROUP BY / ORDER BY and in WHERE: only the WHERE occurrence
-- may be rewritten on the shard, the header of the intermediate stage must not change.
SELECT substring(tags['k1'], 1, 5) FROM t_map_shard_dist WHERE tags['k1'] != '' ORDER BY id LIMIT 1;
SELECT substring(tags['k1'], 201) AS v, count() FROM t_map_shard_dist WHERE tags['k1'] != '' GROUP BY v ORDER BY v SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT substring(tags['k1'], 201) AS v, count() FROM t_map_shard_dist WHERE tags['k1'] != '' GROUP BY v ORDER BY v SETTINGS prefer_localhost_replica = 1, serialize_query_plan = 1;
SELECT id FROM t_map_shard_dist WHERE tags['k1'] != '' ORDER BY id LIMIT 2 SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

SELECT 'read amplification';

SYSTEM FLUSH LOGS query_log;

-- The Distributed queries must read a single key subcolumn (like the direct query does),
-- not the whole Map: several times less data than with the optimization disabled.
WITH
    (SELECT max(read_bytes) FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment = '04838_map_dist_noopt') AS whole_map_bytes,
    (SELECT max(read_bytes) FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment = '04838_map_local') AS local_bytes
SELECT
    log_comment,
    max(read_bytes) * 3 < whole_map_bytes AS reads_subcolumn,
    max(read_bytes) < 3 * local_bytes AS parity_with_local
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('04838_map_dist_local', '04838_map_dist_remote', '04838_map_dist_plan')
GROUP BY log_comment
ORDER BY log_comment;

WITH
    (SELECT max(read_bytes) FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment = '04838_arr_dist_noopt') AS whole_array_bytes
SELECT
    log_comment,
    max(read_bytes) * 3 < whole_array_bytes AS reads_subcolumn
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment = '04838_arr_dist'
GROUP BY log_comment;

DROP TABLE t_map_shard_dist;
DROP TABLE t_map_shard;
