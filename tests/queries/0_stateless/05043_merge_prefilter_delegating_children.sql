-- Tags: shard

-- A `WHERE _table = ...` or `WHERE _database = ...` filter over a `Merge` table silently returned
-- zero rows when the matching child reads its data from other tables (`Distributed`, `Merge`,
-- `Buffer`, `Alias`): the rows of such a child carry the name of the table that actually produced
-- them, while the pruning in `ReadFromMerge::getSelectedTables` matched the predicate against the
-- child's own name. Such children are now always read, and the predicate filters their rows.

DROP TABLE IF EXISTS t05043_leaf;
DROP TABLE IF EXISTS t05043_dist;
DROP TABLE IF EXISTS t05043_inner_leaf;
DROP TABLE IF EXISTS t05043_inner_merge;
DROP TABLE IF EXISTS t05043_buf_dst;
DROP TABLE IF EXISTS t05043_buf;
DROP TABLE IF EXISTS t05043_alias_target;
DROP TABLE IF EXISTS t05043_alias;
DROP TABLE IF EXISTS t05043_plain;
DROP VIEW IF EXISTS t05043_throwing;

SELECT 'Merge over Distributed';
CREATE TABLE t05043_leaf (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t05043_leaf VALUES (1), (2), (3);
CREATE TABLE t05043_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t05043_leaf);

SELECT count() FROM merge(currentDatabase(), '^t05043_dist$') WHERE _table = 't05043_leaf';
SELECT count() FROM merge(currentDatabase(), '^t05043_dist$') WHERE _table = 't05043_dist';
SELECT DISTINCT _table FROM merge(currentDatabase(), '^t05043_dist$');
-- The same at the `FetchColumns` stage (`ARRAY JOIN` prevents forwarding the query to the child):
SELECT count() FROM merge(currentDatabase(), '^t05043_dist$') ARRAY JOIN [1] AS one WHERE _table = 't05043_leaf';

SELECT 'Merge over Merge';
CREATE TABLE t05043_inner_leaf (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t05043_inner_leaf VALUES (10), (20);
CREATE TABLE t05043_inner_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^t05043_inner_leaf$');

SELECT count() FROM merge(currentDatabase(), '^t05043_inner_merge$') WHERE _table = 't05043_inner_leaf';
SELECT count() FROM merge(currentDatabase(), '^t05043_inner_merge$') WHERE _table = 't05043_inner_merge';

SELECT 'Merge over Buffer';
CREATE TABLE t05043_buf_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
-- The time/rows/bytes thresholds are high enough that nothing is flushed during the test.
CREATE TABLE t05043_buf (x UInt64) ENGINE = Buffer(currentDatabase(), t05043_buf_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t05043_buf_dst VALUES (100), (200);
INSERT INTO t05043_buf VALUES (300);

SELECT count() FROM merge(currentDatabase(), '^t05043_buf$') WHERE _table = 't05043_buf_dst';
SELECT count() FROM merge(currentDatabase(), '^t05043_buf$') WHERE _table = 't05043_buf';

SELECT 'Merge over Alias';
CREATE TABLE t05043_alias_target (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t05043_alias_target VALUES (1000);
CREATE TABLE t05043_alias ENGINE = Alias('t05043_alias_target');

SELECT count() FROM merge(currentDatabase(), '^t05043_alias$') WHERE _table = 't05043_alias_target';
SELECT count() FROM merge(currentDatabase(), '^t05043_alias$') WHERE _table = 't05043_alias';

SELECT 'Pruning still works';
-- A child that does not read from other tables is still pruned by its own name:
-- the view throws on read, so the query only succeeds if the view is never read.
CREATE TABLE t05043_plain (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t05043_plain VALUES (1);
CREATE VIEW t05043_throwing AS SELECT throwIf(number >= 0, 'must not be read') + number AS x FROM system.numbers LIMIT 1;

SELECT count() FROM merge(currentDatabase(), '^t05043_(plain|throwing)$') WHERE _table = 't05043_plain';
SELECT count() FROM merge(currentDatabase(), '^t05043_(plain|throwing)$') WHERE _database = currentDatabase() AND _table = 't05043_plain';

DROP TABLE t05043_dist;
DROP TABLE t05043_leaf;
DROP TABLE t05043_inner_merge;
DROP TABLE t05043_inner_leaf;
DROP TABLE t05043_buf;
DROP TABLE t05043_buf_dst;
DROP TABLE t05043_alias;
DROP TABLE t05043_alias_target;
DROP VIEW t05043_throwing;
DROP TABLE t05043_plain;
