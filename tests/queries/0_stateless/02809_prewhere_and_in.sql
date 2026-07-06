DROP TABLE IF EXISTS t_02809;

CREATE TABLE t_02809(a Int64, b Int64, s String)
ENGINE=MergeTree order by tuple()
AS SELECT number, number%10, toString(arrayMap(i-> cityHash64(i*number), range(50))) FROM numbers(10000);

CREATE TABLE t_02809_set(c Int64)
ENGINE=Set()
AS SELECT * FROM numbers(10);

CREATE TABLE t_02809_aux(c Int64)
ENGINE=Memory()
AS SELECT * FROM numbers(10);


SET optimize_move_to_prewhere=1;

-- Queries with 'IN'
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a IN (SELECT * FROM system.one)) WHERE explain LIKE '%Prewhere filter';
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a IN (1,2,3)) WHERE explain LIKE '%Prewhere filter';
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a IN t_02809_set) WHERE explain LIKE '%Prewhere filter';
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a IN t_02809_aux) WHERE explain LIKE '%Prewhere filter';

-- Queries with 'NOT IN'
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a NOT IN (SELECT * FROM system.one)) WHERE explain LIKE '%Prewhere filter';
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a NOT IN (1,2,3)) WHERE explain LIKE '%Prewhere filter';
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a NOT IN t_02809_set) WHERE explain LIKE '%Prewhere filter';
SELECT * FROM (EXPLAIN actions=1 SELECT * FROM t_02809 WHERE a NOT IN t_02809_aux) WHERE explain LIKE '%Prewhere filter';

DROP TABLE t_02809;
DROP TABLE t_02809_set;
DROP TABLE t_02809_aux;
