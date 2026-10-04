-- Regression test: with analyzer_compatibility_join_using_top_level_identifier, a JOIN USING key that
-- resolves to a SELECT-list alias must not keep the name of a VIRTUAL column of the left table
-- expression. The synthesized column used to collide with the virtual column on the same source, so
-- the raw virtual value silently became the join key instead of the aliased expression.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_mg;
DROP TABLE IF EXISTS t_up;
DROP TABLE IF EXISTS t_raw;
DROP TABLE IF EXISTS t_ord0;
DROP TABLE IF EXISTS t_ord1;
DROP TABLE IF EXISTS t_nat;

CREATE TABLE t_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_src VALUES (1);
CREATE TABLE t_mg (x UInt64) ENGINE = Merge(currentDatabase(), '^t_src$');

-- The right-hand fixtures are derived from the actual virtual value, so no part name is hard-coded:
-- t_up holds it uppercased (what the aliased expression yields), t_raw holds it verbatim (what an
-- un-renamed virtual column yields). A join can match at most one of them.
CREATE TABLE t_up (`_part` String) ENGINE = Memory;
CREATE TABLE t_raw (`_part` String) ENGINE = Memory;
INSERT INTO t_up SELECT upper(_part) FROM t_src;
INSERT INTO t_raw SELECT _part FROM t_src;

CREATE TABLE t_ord0 (`_t` String) ENGINE = Memory;
CREATE TABLE t_ord1 (`_t` String) ENGINE = Memory;
INSERT INTO t_ord0 SELECT _part FROM t_src;
INSERT INTO t_ord1 SELECT upper(_part) FROM t_src;

-- t_nat shares no named column with t_src, so a NATURAL JOIN of the two must be a cross join.
-- PARTITION BY gives it a different part name, so if virtual columns ever entered the NATURAL JOIN
-- name intersection the join would match on `_part` instead and return no rows.
CREATE TABLE t_nat (y UInt64) ENGINE = MergeTree PARTITION BY y ORDER BY y;
INSERT INTO t_nat VALUES (1);

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

-- The two fixtures must differ, otherwise every assertion below is degenerate.
SELECT 'fixture-discriminates', upper(_part) != _part FROM t_src;

-- The aliased expression must be the join key. Each of the three table-expression kinds
-- (table, Merge engine, merge() table function) used to return 0 rows here.
SELECT 'alias-is-key-table', count() FROM (SELECT upper(a._part) AS _part FROM t_src AS a JOIN t_up USING (_part));
SELECT 'alias-is-key-merge-engine', count() FROM (SELECT upper(m._part) AS _part FROM t_mg AS m JOIN t_up USING (_part));
SELECT 'alias-is-key-merge-function', count() FROM (SELECT upper(m._part) AS _part FROM merge(currentDatabase(), '^t_src$') AS m JOIN t_up USING (_part));

-- Converse of the above: the raw virtual value must NOT be the join key. These used to return 1 row.
SELECT 'raw-virtual-not-key-table', count() FROM (SELECT upper(a._part) AS _part FROM t_src AS a JOIN t_raw USING (_part));
SELECT 'raw-virtual-not-key-merge-engine', count() FROM (SELECT upper(m._part) AS _part FROM t_mg AS m JOIN t_raw USING (_part));
SELECT 'raw-virtual-not-key-merge-function', count() FROM (SELECT upper(m._part) AS _part FROM merge(currentDatabase(), '^t_src$') AS m JOIN t_raw USING (_part));

-- Paths that were already correct must not move.
SELECT 'ordinary-column-collision', count() FROM (SELECT upper(o._t) AS _t FROM t_ord0 AS o JOIN t_ord1 USING (_t));
SELECT 'subquery-left-side', count() FROM (SELECT upper(s._part) AS _part FROM (SELECT _part FROM t_src) AS s JOIN t_up USING (_part));
SELECT 'natural-join-ignores-virtuals', count() FROM (SELECT * FROM t_src NATURAL JOIN t_nat);
SELECT 'bare-virtual-using-key', count() FROM (SELECT x FROM t_src AS a JOIN t_raw USING (_part));
SELECT 'compatibility-setting-off', count() FROM (SELECT upper(a._part) AS _part FROM t_src AS a JOIN t_up USING (_part)) SETTINGS analyzer_compatibility_join_using_top_level_identifier = 0;

-- Mechanism: the synthesized column is renamed away from the virtual name.
SELECT 'synthesized-column-renamed', count() > 0 FROM (
    EXPLAIN QUERY TREE SELECT upper(a._part) AS _part FROM t_src AS a JOIN t_up USING (_part)
) WHERE explain ILIKE '%column_name: __part%';
