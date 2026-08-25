DROP TABLE IF EXISTS sharded_table;
CREATE TABLE sharded_table (dummy UInt8) ENGINE = Distributed('test_cluster_two_shards', 'system', 'one');

SET prefer_localhost_replica=0;
SELECT * FROM sharded_table FORMAT Null SETTINGS log_comment='02985_shard_query_start_time_query_1';

SYSTEM FLUSH LOGS query_log;

-- Check that there are 2 queries to shards and for each one query_start_time_microseconds is more recent
-- than initial_query_start_time_microseconds, and initial_query_start_time_microseconds matches the original query
-- query_start_time_microseconds
WITH
(
    SELECT
        (query_id, query_start_time, query_start_time_microseconds)
    FROM
        system.query_log
    WHERE
          event_date >= yesterday()
      AND current_database = currentDatabase()
      AND log_comment = '02985_shard_query_start_time_query_1'
      AND type = 'QueryFinish'
) AS id_and_start_tuple
SELECT
    type,
    countIf(query_start_time >= initial_query_start_time), -- Using >= because it's comparing seconds
    countIf(query_start_time_microseconds > initial_query_start_time_microseconds),
    countIf(initial_query_start_time = id_and_start_tuple.2),
    countIf(initial_query_start_time_microseconds = id_and_start_tuple.3)
FROM
    system.query_log
WHERE
    NOT is_initial_query AND initial_query_id = id_and_start_tuple.1
GROUP BY type;
