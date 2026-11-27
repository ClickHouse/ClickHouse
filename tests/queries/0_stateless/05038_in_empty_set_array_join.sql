-- Random settings limits: optimize_move_to_prewhere=(1, 1); query_plan_optimize_prewhere=(1, 1)

DROP TABLE IF EXISTS t_in_empty_set;
DROP TABLE IF EXISTS t_in_empty_set_nullable;
DROP TABLE IF EXISTS t_in_empty_set_lc;
DROP TABLE IF EXISTS t_in_empty_set_storage;
DROP TABLE IF EXISTS t_in_empty_set_pk;

CREATE TABLE t_in_empty_set (a Int, b Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in_empty_set VALUES (1, 2);

CREATE TABLE t_in_empty_set_pk (a Int, b Int) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_in_empty_set_pk VALUES (1, 2);

SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a NOT IN ();
SELECT x FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a NOT IN ();
SELECT count() FROM t_in_empty_set LEFT ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN _CAST([], 'Array(Int32)');
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE toString(a) IN ();
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE multiIf(a IN (), 1, 0);
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a GLOBAL NOT IN ();

SELECT count() FROM t_in_empty_set WHERE a IN ();
SELECT a IN (), a NOT IN () FROM t_in_empty_set;

SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN (SELECT number FROM numbers(3));
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN (SELECT a FROM t_in_empty_set WHERE 0);

CREATE TABLE t_in_empty_set_nullable (a Nullable(Int), b Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in_empty_set_nullable VALUES (1, 2), (NULL, 3);

SELECT count() FROM t_in_empty_set_nullable ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set_nullable ARRAY JOIN [b] AS x WHERE nullIn(a, ());
SELECT a, a IN (), a NOT IN () FROM t_in_empty_set_nullable ORDER BY a NULLS LAST;
SELECT a, nullIn(a, ()), notNullIn(a, ()) FROM t_in_empty_set_nullable ORDER BY a NULLS LAST;

CREATE TABLE t_in_empty_set_lc (a LowCardinality(String), b Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in_empty_set_lc VALUES ('x', 2);

SELECT count() FROM t_in_empty_set_lc ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set_lc ARRAY JOIN [b] AS x WHERE a NOT IN ();
SELECT toTypeName(a IN ()) FROM t_in_empty_set_lc;

CREATE TABLE t_in_empty_set_storage (x Int) ENGINE = Set;

-- A `Set` table can be filled after a query referencing it is planned, so its emptiness must not
-- be folded into a constant. The plan keeps the function while a literal empty list becomes one.
-- The old analyzer folds both, so the plan-shape assertions below pin the new one.
SET enable_analyzer = 1;
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(b) FROM t_in_empty_set WHERE a IN ()) WHERE explain ILIKE '%Filter column: 0%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(b) FROM t_in_empty_set WHERE a IN t_in_empty_set_storage) WHERE explain ILIKE '%Filter column: 0%';
-- Only the filter column names the set: a `ReadFromRemote*` step echoes the whole remote query,
-- so an unscoped match counts a second line whenever the read is distributed.
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(b) FROM t_in_empty_set WHERE a IN t_in_empty_set_storage) WHERE explain ILIKE '%filter column:%' AND explain ILIKE '%t_in_empty_set_storage%';

SELECT count() FROM t_in_empty_set WHERE a IN t_in_empty_set_storage;
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN t_in_empty_set_storage;
INSERT INTO t_in_empty_set_storage VALUES (1);
SELECT count() FROM t_in_empty_set WHERE a IN t_in_empty_set_storage;
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN t_in_empty_set_storage;

-- An empty subquery set is not folded either: each shard still runs index analysis against it, which
-- needs the function in the plan. Folding it away leaves one `0-element set` report instead of two.
SET serialize_query_plan = 0, enable_parallel_replicas = 0, prefer_localhost_replica = 1, optimize_skip_unused_shards = 0;
SELECT count() FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT sum(b) FROM (SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), t_in_empty_set_pk))
    WHERE a IN (SELECT toInt32(number) FROM numbers(0))
) WHERE explain ILIKE '%0-element set%';

DROP TABLE t_in_empty_set;
DROP TABLE t_in_empty_set_nullable;
DROP TABLE t_in_empty_set_lc;
DROP TABLE t_in_empty_set_storage;
DROP TABLE t_in_empty_set_pk;
