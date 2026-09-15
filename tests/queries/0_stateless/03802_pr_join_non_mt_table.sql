SET enable_parallel_replicas=1,
    max_parallel_replicas=3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'parallel_replicas';

SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 ( `Id` UInt64, `Payload` String, `Time` DateTime ) ENGINE = Memory;
INSERT INTO t0 SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(10);

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ( `EventId` UInt64, `Attribute` String ) ENGINE = MergeTree ORDER BY EventId;
INSERT INTO t1 SELECT 32 AS EventId, 'attr' AS Attribute;

SELECT '-- t0 count()';
SELECT count() FROM t0;

SELECT '-- t1 count()';
SELECT count() FROM t1;

SELECT '-- inner join t0, t1';
SELECT count()
FROM t0 INNER JOIN t1 ON t1.EventId = t0.Id;

SELECT '-- inner join t1, t0';
SELECT count()
FROM t1 INNER JOIN t0 ON t1.EventId = t0.Id;

SELECT '-- left join t0, t1';
SELECT count()
FROM t0 LEFT JOIN t1 ON t1.EventId = t0.Id;

SELECT '-- left join t1, t0';
SELECT count()
FROM t1 LEFT JOIN t0 ON t1.EventId = t0.Id;

-- parallel replicas is not supported currently for RIGHT JOIN with non_mt_table on left side, but it should work w/o error
SELECT '-- right join t0, t1';
SELECT count()
FROM t0 RIGHT JOIN t1 ON t1.EventId = t0.Id;

SELECT '-- right join t1, t0';
SELECT count()
FROM t1 RIGHT JOIN t0 ON t1.EventId = t0.Id;

DROP TABLE t0;
DROP TABLE t1;
