-- Tags: no-fasttest
-- Test: Verify MV validation with MATERIALIZED columns in target table
-- The getSampleBlockInsertable() change means MATERIALIZED columns are excluded
-- from the validation set. This test covers that interaction.

DROP TABLE IF EXISTS src_04089;
DROP TABLE IF EXISTS dst_04089;
DROP TABLE IF EXISTS mv_04089;

CREATE TABLE src_04089 (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_04089 (x Int32, m Int32 MATERIALIZED x * 10) ENGINE = MergeTree ORDER BY x;

-- With default (allow_materialized_view_with_bad_select=false):
-- MV selecting a column matching MATERIALIZED column name should be rejected
-- because getSampleBlockInsertable() excludes MATERIALIZED columns
CREATE MATERIALIZED VIEW mv_04089 TO dst_04089 AS SELECT x, x + 1 AS m FROM src_04089; -- { serverError THERE_IS_NO_COLUMN }

-- MV selecting only insertable columns should succeed
CREATE MATERIALIZED VIEW mv_04089 TO dst_04089 AS SELECT x FROM src_04089;

INSERT INTO src_04089 VALUES (5, 100);

-- Verify: x=5, m computed by MATERIALIZED expr = 5*10 = 50
SELECT x, m FROM dst_04089 ORDER BY x;

DROP TABLE mv_04089;

-- With allow_materialized_view_with_bad_select=true:
-- MV selecting MATERIALIZED column name should be allowed (skipped during validation)
SET allow_materialized_view_with_bad_select = true;
CREATE MATERIALIZED VIEW mv_04089 TO dst_04089 AS SELECT x, x + 1 AS m FROM src_04089;
SET allow_materialized_view_with_bad_select = false;

INSERT INTO src_04089 VALUES (7, 200);

-- Verify: x=7, m gets value from MV SELECT (7+1=8), overriding MATERIALIZED expression
SELECT x, m FROM dst_04089 WHERE x = 7 ORDER BY x;

DROP TABLE mv_04089;
DROP TABLE dst_04089;
DROP TABLE src_04089;
