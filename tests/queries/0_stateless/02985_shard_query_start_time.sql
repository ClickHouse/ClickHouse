DROP TABLE IF EXISTS sharded_table;
CREATE TABLE sharded_table (dummy UInt8) ENGINE = Distributed('test_cluster_two_shards', 'system', 'one');

SELECT * FROM sharded_table FORMAT Null SETTINGS log_comment='02985_shard_query_start_time_query_1';

SYSTEM FLUSH LOGS;

-- We do not test for query_start_time because that would conflict pretty easily
WITH
(
    SELECT
        (query_id, query_start_time_microseconds)
    FROM
        system.query_log
    WHERE
            event_date >= yesterday()
      AND current_database = currentDatabase()
      AND log_comment = '02985_shard_query_start_time_query_1'
      AND type = 'QueryFinish'
    ORDER BY query_start_time_microseconds DESC
    LIMIT 1
) AS id_and_start_tuple
SELECT
    query_start_time_microseconds > initial_query_start_time_microseconds,
    initial_query_start_time_microseconds = id_and_start_tuple.2
FROM
    system.query_log
WHERE
    NOT is_initial_query AND initial_query_id = id_and_start_tuple.1;
