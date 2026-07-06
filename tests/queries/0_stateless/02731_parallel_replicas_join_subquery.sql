-- Tags: zookeeper

DROP TABLE IF EXISTS join_inner_table SYNC;

CREATE TABLE join_inner_table
(
    id UUID,
    key String,
    number Int64,
    value1 String,
    value2 String,
    time Int64
)
ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/join_inner_table', 'r1')
ORDER BY (id, number, key);

INSERT INTO join_inner_table
SELECT
    '833c9e22-c245-4eb5-8745-117a9a1f26b1'::UUID as id,
    rowNumberInAllBlocks()::String as key,
    * FROM generateRandom('number Int64, value1 String, value2 String, time Int64', 1, 10, 2)
LIMIT 100;

SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET joined_subquery_requires_alias = 0;

SELECT '=============== INNER QUERY (NO PARALLEL) ===============';

SELECT
    key,
    value1,
    value2,
    toUInt64(min(time)) AS start_ts
FROM join_inner_table
    PREWHERE (id = '833c9e22-c245-4eb5-8745-117a9a1f26b1') AND (number > toUInt64('1610517366120'))
GROUP BY key, value1, value2
ORDER BY key, value1, value2
LIMIT 10;

SELECT '=============== no-analyzer: INNER QUERY (PARALLEL), QUERIES EXECUTED BY PARALLEL INNER QUERY ALONE ===============';

-- Parallel inner query alone without analyzer
SELECT
    key,
    value1,
    value2,
    toUInt64(min(time)) AS start_ts
FROM join_inner_table
PREWHERE (id = '833c9e22-c245-4eb5-8745-117a9a1f26b1') AND (number > toUInt64('1610517366120'))
GROUP BY key, value1, value2
ORDER BY key, value1, value2
LIMIT 10
SETTINGS enable_parallel_replicas = 1, enable_analyzer=0, parallel_replicas_only_with_analyzer=0;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasQueryCount'], replaceRegexpAll(query, '_data_(\d+)_(\d+)', '_data_') as query
FROM system.query_log
WHERE
      event_date >= yesterday()
  AND type = 'QueryFinish'
  AND query_id IN
      (
          SELECT query_id
          FROM system.query_log
          WHERE
                current_database = currentDatabase()
            AND event_date >= yesterday()
            AND type = 'QueryFinish'
            AND query LIKE '-- Parallel inner query alone without analyzer%'
      );

SELECT '=============== analyzer: INNER QUERY (PARALLEL), QUERIES EXECUTED BY PARALLEL INNER QUERY ALONE ===============';

-- Parallel inner query alone with analyzer
SELECT
    key,
    value1,
    value2,
    toUInt64(min(time)) AS start_ts
FROM join_inner_table
PREWHERE (id = '833c9e22-c245-4eb5-8745-117a9a1f26b1') AND (number > toUInt64('1610517366120'))
GROUP BY key, value1, value2
ORDER BY key, value1, value2
LIMIT 10
SETTINGS enable_parallel_replicas = 1, enable_analyzer=1;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasQueryCount'], replaceRegexpAll(query, '_data_(\d+)_(\d+)', '_data_') as query
FROM system.query_log
WHERE
      event_date >= yesterday()
  AND type = 'QueryFinish'
  AND query_id IN
      (
          SELECT query_id
          FROM system.query_log
          WHERE
                current_database = currentDatabase()
            AND event_date >= yesterday()
            AND type = 'QueryFinish'
            AND query LIKE '-- Parallel inner query alone with analyzer%'
      );

---- Query with JOIN

DROP TABLE IF EXISTS join_outer_table SYNC;

CREATE TABLE join_outer_table
(
    id UUID,
    key String,
    otherValue1 String,
    otherValue2 String,
    time Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/join_outer_table', 'r1')
ORDER BY (id, time, key);

INSERT INTO join_outer_table
SELECT
    '833c9e22-c245-4eb5-8745-117a9a1f26b1'::UUID as id,
        (rowNumberInAllBlocks() % 10)::String as key,
        * FROM generateRandom('otherValue1 String, otherValue2 String, time Int64', 1, 10, 2)
LIMIT 100;


SELECT '=============== OUTER QUERY (NO PARALLEL) ===============';

SELECT
    value1,
    value2,
    avg(count) AS avg
FROM
(
    SELECT
        key,
        value1,
        value2,
        count() AS count
    FROM join_outer_table
    INNER JOIN
    (
        SELECT
            key,
            value1,
            value2,
            toUInt64(min(time)) AS start_ts
        FROM join_inner_table
        PREWHERE (id = '833c9e22-c245-4eb5-8745-117a9a1f26b1') AND (number > toUInt64('1610517366120'))
        GROUP BY key, value1, value2
    ) USING (key)
    GROUP BY key, value1, value2
)
GROUP BY value1, value2
ORDER BY value1, value2;

SELECT '=============== no-analyzer: OUTER QUERY (PARALLEL) ===============';

-- Parallel full query without analyzer
SELECT
    value1,
    value2,
    avg(count) AS avg
FROM
    (
        SELECT
            key,
            value1,
            value2,
            count() AS count
        FROM join_outer_table
        INNER JOIN
        (
            SELECT
                key,
                value1,
                value2,
                toUInt64(min(time)) AS start_ts
            FROM join_inner_table
            PREWHERE (id = '833c9e22-c245-4eb5-8745-117a9a1f26b1') AND (number > toUInt64('1610517366120'))
            GROUP BY key, value1, value2
        ) USING (key)
        GROUP BY key, value1, value2
        )
GROUP BY value1, value2
ORDER BY value1, value2
SETTINGS enable_parallel_replicas = 1, enable_analyzer=0, parallel_replicas_only_with_analyzer=0;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasQueryCount'], replaceRegexpAll(query, '_data_(\d+)_(\d+)', '_data_') as query
FROM system.query_log
WHERE
      event_date >= yesterday()
  AND type = 'QueryFinish'
  AND query_id IN
      (
          SELECT query_id
          FROM system.query_log
          WHERE
                current_database = currentDatabase()
            AND event_date >= yesterday()
            AND type = 'QueryFinish'
            AND query LIKE '-- Parallel full query without analyzer%'
      );

SELECT '=============== analyzer: OUTER QUERY (PARALLEL) ===============';

-- Parallel full query with analyzer
SELECT
    value1,
    value2,
    avg(count) AS avg
FROM
    (
        SELECT
            key,
            value1,
            value2,
            count() AS count
        FROM join_outer_table
        INNER JOIN
        (
            SELECT
                key,
                value1,
                value2,
                toUInt64(min(time)) AS start_ts
            FROM join_inner_table
            PREWHERE (id = '833c9e22-c245-4eb5-8745-117a9a1f26b1') AND (number > toUInt64('1610517366120'))
            GROUP BY key, value1, value2
        ) USING (key)
        GROUP BY key, value1, value2
        )
GROUP BY value1, value2
ORDER BY value1, value2
SETTINGS enable_parallel_replicas = 1, enable_analyzer=1;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasQueryCount'], replaceRegexpAll(query, '_data_(\d+)_(\d+)', '_data_') as query
FROM system.query_log
WHERE
      event_date >= yesterday()
  AND type = 'QueryFinish'
  AND query_id IN
      (
          SELECT query_id
          FROM system.query_log
          WHERE
                current_database = currentDatabase()
            AND event_date >= yesterday()
            AND type = 'QueryFinish'
            AND query LIKE '-- Parallel full query with analyzer%'
      );
