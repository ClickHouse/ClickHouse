-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/122370
-- With `analyzer_compatibility_join_using_top_level_identifier`, a JOIN USING key resolved from a SELECT-list alias
-- gets a synthesized column name that is prefixed with `_` until it no longer clashes with a column of the left table.
-- The synthesized name must not clash with a name already taken by an earlier USING key either: two keys named like
-- `k` and `_k` used to end up as two columns named `_k`, so one expression served as both keys (wrong result) or the
-- query failed with AMBIGUOUS_COLUMN_NAME.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_l2;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k Int32, v Int32) ENGINE = Memory;
CREATE TABLE t_l2 (k Int32, _k Int32, v Int32) ENGINE = Memory;
CREATE TABLE t_r (k Int32, _k Int32) ENGINE = Memory;
INSERT INTO t_l VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO t_l2 VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30), (4, 400, 40);
INSERT INTO t_r VALUES (1, 10), (2, 20), (3, 30), (1, 100);

SET analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 'setting on, USING (k, _k)';
SELECT k::Int64 AS k, v AS _k FROM t_l JOIN t_r USING (k, _k) ORDER BY ALL;
SELECT 'setting on, USING (_k, k)';
SELECT v AS _k, k::Int64 AS k FROM t_l JOIN t_r USING (_k, k) ORDER BY ALL;

SELECT 'setting on, LEFT JOIN';
SELECT k::Int64 AS k, v AS _k, t_r.k FROM t_l LEFT JOIN t_r USING (k, _k) ORDER BY ALL;
SELECT v AS _k, k::Int64 AS k, t_r.k FROM t_l LEFT JOIN t_r USING (_k, k) ORDER BY ALL;

SELECT 'setting on, FULL JOIN with join_use_nulls';
SELECT k::Int64 AS k, v AS _k FROM t_l FULL JOIN t_r USING (k, _k) ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT v AS _k, k::Int64 AS k FROM t_l FULL JOIN t_r USING (_k, k) ORDER BY ALL SETTINGS join_use_nulls = 1;

SELECT 'setting on, merge()';
SELECT k::Int64 AS k, v AS _k FROM merge(currentDatabase(), '^t_l$') AS m JOIN t_r USING (k, _k) ORDER BY ALL;
SELECT v AS _k, k::Int64 AS k FROM merge(currentDatabase(), '^t_l$') AS m JOIN t_r USING (_k, k) ORDER BY ALL;

-- The left table has both `k` and `_k`, so both keys are renamed past the table columns and would meet at `__k`.
SELECT 'setting on, left table has k and _k';
SELECT k::Int64 AS k, v AS _k FROM t_l2 JOIN t_r USING (k, _k) ORDER BY ALL;
SELECT v AS _k, k::Int64 AS k FROM t_l2 JOIN t_r USING (_k, k) ORDER BY ALL;

SELECT 'setting on, NATURAL JOIN';
SELECT k::Int64 AS k, v AS _k FROM t_l2 NATURAL JOIN t_r ORDER BY ALL;
SELECT v AS _k, k::Int64 AS k FROM t_l2 NATURAL JOIN t_r ORDER BY ALL;

SET analyzer_compatibility_join_using_top_level_identifier = 0;

-- Without the setting, the keys are the columns of the left table, not the aliases.
SELECT 'setting off, left table has k and _k';
SELECT k::Int64 AS k, v AS _k FROM t_l2 JOIN t_r USING (k, _k) ORDER BY ALL;
SELECT v AS _k, k::Int64 AS k FROM t_l2 JOIN t_r USING (_k, k) ORDER BY ALL;

SELECT 'setting off, NATURAL JOIN';
SELECT k::Int64 AS k, v AS _k FROM t_l2 NATURAL JOIN t_r ORDER BY ALL;

SELECT 'setting off, the left table has no _k';
SELECT k::Int64 AS k, v AS _k FROM t_l JOIN t_r USING (k, _k) ORDER BY ALL; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE t_l;
DROP TABLE t_l2;
DROP TABLE t_r;
