-- Tags: shard

-- The `Merge` engine's `_database`/`_table` virtual columns carry the name of the Merge table's
-- own child - the same name the `WHERE _table = ...` child-pruning filter matches - even when the
-- child delegates its read to another table. Before this was fixed, a child that delegates
-- (`Distributed`, a nested `Merge`, `Buffer`) had its rows stamped by whatever storage eventually
-- read them (e.g. the remote table on the shards), so a filter on the child's name silently
-- returned no rows, and a filter on the underlying table's name returned rows the pruning was
-- supposed to drop.

DROP TABLE IF EXISTS t_leaf;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS m_over_dist;
DROP TABLE IF EXISTS m_inner;
DROP TABLE IF EXISTS m_outer;
DROP TABLE IF EXISTS t_buf;
DROP TABLE IF EXISTS m_over_buf;
DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS m_same;
DROP TABLE IF EXISTS t_alias_a;
DROP TABLE IF EXISTS t_alias_b;
DROP TABLE IF EXISTS m_alias;

CREATE TABLE t_leaf (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_leaf SELECT toString(number % 3) FROM numbers(6);

CREATE TABLE t_dist AS t_leaf
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_leaf);
CREATE TABLE m_over_dist (s String) ENGINE = Merge(currentDatabase(), '^t_dist$');

SELECT '-- rows carry the child name, not the remote table name';
SELECT DISTINCT _table, _database = currentDatabase() FROM m_over_dist;
SELECT DISTINCT _table FROM m_over_dist SETTINGS enable_analyzer = 0;

SELECT '-- a remote GROUP BY over the virtual column';
-- Only the value of the grouping key is under test here - it must be the child's name, not the
-- remote table's, and it must survive the shard-side `GROUP BY`. Whether the initiator merges the
-- shards' partial aggregates into one row or returns them separately depends on the plan the
-- distributed aggregation picks, so the counts are summed up again above the `Merge`.
SELECT _table, sum(c) FROM (SELECT _table, count() AS c FROM m_over_dist GROUP BY _table) GROUP BY _table;

SELECT '-- a filter on the child name keeps all rows, both analyzers';
SELECT count() FROM m_over_dist WHERE _table = 't_dist';
SELECT count() FROM m_over_dist WHERE _table = 't_dist' SETTINGS enable_analyzer = 0;

SELECT '-- the remote table name matches no child';
SELECT count() FROM m_over_dist WHERE _table = 't_leaf';

SELECT '-- an expression over the virtual column, computed on the shards';
SELECT DISTINCT concat(_table, '/', s) AS v FROM m_over_dist ORDER BY v;

SELECT '-- a JOIN above the Merge must not turn the values into defaults';
SELECT DISTINCT _table, s FROM m_over_dist LEFT JOIN (SELECT '0' AS s) AS t USING (s) ORDER BY s;

SELECT '-- each Merge level reports its own child';
CREATE TABLE m_inner (s String) ENGINE = Merge(currentDatabase(), '^t_leaf$');
CREATE TABLE m_outer (s String) ENGINE = Merge(currentDatabase(), '^m_inner$');
SELECT DISTINCT _table FROM m_outer;
SELECT count() FROM m_outer WHERE _table = 'm_inner';
SELECT DISTINCT _table FROM m_inner;
SELECT count() FROM m_inner WHERE _table = 't_leaf';

SELECT '-- a Buffer child is also reported under its own name';
CREATE TABLE t_buf AS t_leaf
    ENGINE = Buffer(currentDatabase(), t_leaf, 1, 100, 100, 100, 100, 10000000, 100000000);
CREATE TABLE m_over_buf (s String) ENGINE = Merge(currentDatabase(), '^t_buf$');
SELECT DISTINCT _table FROM m_over_buf;
SELECT count() FROM m_over_buf WHERE _table = 't_buf';

-- A correlated reference has to be qualified, otherwise `_table` resolves against the subquery's
-- own table. A `Distributed` child is not covered: correlated subqueries are rejected outright for
-- remote tables.
SELECT '-- a correlated subquery over the virtual column also sees the child name';
SELECT count() FROM m_outer WHERE EXISTS (SELECT 1 FROM numbers(1) WHERE m_outer._table = 'm_inner') SETTINGS enable_analyzer = 1;
SELECT count() FROM m_outer WHERE EXISTS (SELECT 1 FROM numbers(1) WHERE m_outer._table = 't_leaf') SETTINGS enable_analyzer = 1;
SELECT count() FROM m_over_buf WHERE EXISTS (SELECT 1 FROM numbers(1) WHERE m_over_buf._table = 't_buf') SETTINGS enable_analyzer = 1;
SELECT count() FROM m_over_buf WHERE EXISTS (SELECT 1 FROM numbers(1) WHERE m_over_buf._table = 't_leaf') SETTINGS enable_analyzer = 1;

SELECT '-- same-structure children never share a rewritten query that carries one child name';
CREATE TABLE t_a (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_b (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_c (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_a VALUES (1);
INSERT INTO t_b VALUES (2);
INSERT INTO t_c VALUES (3);
CREATE TABLE m_same (x UInt8) ENGINE = Merge(currentDatabase(), '^t_[abc]$');
SELECT x FROM m_same WHERE _table = 't_b';
SELECT x FROM m_same WHERE _table != 't_a' ORDER BY x;
SELECT x FROM m_same WHERE substring(_table, 3, 1) = 'c';
SELECT _table, x FROM m_same ORDER BY x;

-- An `ALIAS` column whose expression is a virtual column only resolves with the analyzer.
SET enable_analyzer = 1;
SELECT '-- an ALIAS column over the child own _table resolves per child';
CREATE TABLE t_alias_a (x UInt8, child String ALIAS _table) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_alias_b (x UInt8, child String ALIAS _table) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_alias_a VALUES (1);
INSERT INTO t_alias_b VALUES (2);
CREATE TABLE m_alias (x UInt8, child String) ENGINE = Merge(currentDatabase(), '^t_alias_[ab]$');
SELECT child FROM m_alias WHERE child = 't_alias_b';
SELECT x, child FROM m_alias ORDER BY x;

DROP TABLE m_alias;
DROP TABLE t_alias_b;
DROP TABLE t_alias_a;
DROP TABLE m_same;
DROP TABLE t_c;
DROP TABLE t_b;
DROP TABLE t_a;
DROP TABLE m_over_buf;
DROP TABLE t_buf;
DROP TABLE m_outer;
DROP TABLE m_inner;
DROP TABLE m_over_dist;
DROP TABLE t_dist;
DROP TABLE t_leaf;
