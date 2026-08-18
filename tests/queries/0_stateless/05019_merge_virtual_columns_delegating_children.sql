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

CREATE TABLE t_leaf (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_leaf SELECT toString(number % 3) FROM numbers(6);

CREATE TABLE t_dist AS t_leaf
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_leaf);
CREATE TABLE m_over_dist (s String) ENGINE = Merge(currentDatabase(), '^t_dist$');

SELECT '-- rows carry the child name, not the remote table name';
SELECT DISTINCT _table, _database = currentDatabase() FROM m_over_dist;
SELECT DISTINCT _table FROM m_over_dist SETTINGS enable_analyzer = 0;

SELECT '-- a remote GROUP BY over the virtual column';
SELECT _table, count() FROM m_over_dist GROUP BY _table;

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

DROP TABLE m_over_buf;
DROP TABLE t_buf;
DROP TABLE m_outer;
DROP TABLE m_inner;
DROP TABLE m_over_dist;
DROP TABLE t_dist;
DROP TABLE t_leaf;
