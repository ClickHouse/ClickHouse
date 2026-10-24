-- Unique sorting keys skip the aggregating merge on insert. A repeated key still merges min and max.

SET log_queries = 1;
SET log_profile_events = 1;
SET async_insert = 0;
SET optimize_on_insert = 1;

CREATE TABLE t
(
    k UInt64,
    lo SimpleAggregateFunction(min, UInt64),
    hi SimpleAggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY k;

SET log_comment = 'agg_unique_keys';
INSERT INTO t VALUES (3, 30, 300), (1, 10, 100), (2, 20, 200);

SELECT * FROM t;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['MergeTreeDataWriterAggregatingBlocksWithUniqueKeys']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'agg_unique_keys'
    AND query LIKE 'INSERT INTO t VALUES%';

SET log_comment = 'agg_duplicate_keys';
INSERT INTO t VALUES (5, 10, 10), (5, 20, 20);

SELECT k, lo, hi FROM t WHERE k = 5;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['MergeTreeDataWriterAggregatingBlocksWithUniqueKeys']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'agg_duplicate_keys'
    AND query LIKE 'INSERT INTO t VALUES%';

DROP TABLE t;

-- A container-typed SimpleAggregateFunction column keeps the merge, so sumMap normalizes the value.

CREATE TABLE t2
(
    k UInt64,
    m SimpleAggregateFunction(sumMap, Map(UInt64, UInt64))
)
ENGINE = AggregatingMergeTree
ORDER BY k;

SET log_comment = 'agg_container_keys';
INSERT INTO t2 VALUES (1, map(10, 1, 2, 1));

SELECT * FROM t2;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['MergeTreeDataWriterAggregatingBlocksWithUniqueKeys']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'agg_container_keys'
    AND query LIKE 'INSERT INTO t2 VALUES%';

DROP TABLE t2;

-- A nested container-valued SimpleAggregateFunction still needs the merge when tuple elements aggregate.

CREATE TABLE t3
(
    k UInt64,
    m Tuple(m SimpleAggregateFunction(sumMap, Tuple(Array(UInt64), Array(UInt64))))
)
ENGINE = AggregatingMergeTree
ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

SET log_comment = 'agg_tuple_container_keys';
INSERT INTO t3 VALUES (1, tuple(([10, 2], [1, 1])));

SELECT * FROM t3;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['MergeTreeDataWriterAggregatingBlocksWithUniqueKeys']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'agg_tuple_container_keys'
    AND query LIKE 'INSERT INTO t3 VALUES%';

DROP TABLE t3;
