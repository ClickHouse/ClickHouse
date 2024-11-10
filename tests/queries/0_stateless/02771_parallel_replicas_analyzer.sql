-- Tags: zookeeper
DROP TABLE IF EXISTS join_inner_table__fuzz_146_replicated;
CREATE TABLE join_inner_table__fuzz_146_replicated
(
    `id` UUID,
    `key` String,
    `number` Int64,
    `value1` String,
    `value2` String,
    `time` Nullable(Int64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/join_inner_table__fuzz_146_replicated', '{replica}')
ORDER BY (id, number, key)
SETTINGS index_granularity = 8192;

INSERT INTO join_inner_table__fuzz_146_replicated
    SELECT CAST('833c9e22-c245-4eb5-8745-117a9a1f26b1', 'UUID') AS id, CAST(rowNumberInAllBlocks(), 'String') AS key, *
    FROM generateRandom('number Int64, value1 String, value2 String, time Int64', 1, 10, 2) LIMIT 10;

SET parallel_replicas_local_plan = 1;

-- Simple query with analyzer and pure parallel replicas
SELECT number
FROM join_inner_table__fuzz_146_replicated
    SETTINGS
    enable_analyzer = 1,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    enable_parallel_replicas = 1;

SYSTEM FLUSH LOGS;
-- There should be 2 different queries
-- The initial query
-- The query sent to each replica (which should appear 2 times as we are setting max_parallel_replicas to 2)
SELECT
        is_initial_query,
        count() as c, query,
FROM system.query_log
WHERE
        event_date >= yesterday()
  AND type = 'QueryFinish'
  AND initial_query_id =
      (
          SELECT query_id
          FROM system.query_log
          WHERE
                  current_database = currentDatabase()
            AND event_date >= yesterday()
            AND type = 'QueryFinish'
            AND query LIKE '-- Simple query with analyzer and pure parallel replicas%'
      )
GROUP BY is_initial_query, query
ORDER BY is_initial_query DESC, c, query;

DROP TABLE join_inner_table__fuzz_146_replicated;
