-- Tags: zookeeper
DROP TABLE IF EXISTS join_inner_table__fuzz_146_replicated SYNC;
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

-- Simple query with analyzer and pure parallel replicas
SELECT number
FROM join_inner_table__fuzz_146_replicated
    SETTINGS
    enable_analyzer = 1,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    enable_parallel_replicas = 1;

SYSTEM FLUSH LOGS query_log;
SELECT is_initial_query, ProfileEvents['ParallelReplicasQueryCount'] as c, query
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND query_id =
      (
          SELECT query_id
          FROM system.query_log
          WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND type = 'QueryFinish'
            AND query LIKE '-- Simple query with analyzer and pure parallel replicas%'
      );

DROP TABLE join_inner_table__fuzz_146_replicated SYNC;
