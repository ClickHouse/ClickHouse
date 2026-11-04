-- Inside an ordinary view referenced from a materialized view query, the `IN` subquery of a row policy reads the
-- source table, not the inserted block, whether the policy's table is read directly or through a `Merge` table.

DROP TABLE IF EXISTS mv_05345;
DROP TABLE IF EXISTS dst_05345;
DROP VIEW IF EXISTS v_merge_05345;
DROP VIEW IF EXISTS v_direct_05345;
DROP TABLE IF EXISTS merge_05345;
DROP TABLE IF EXISTS child_05345;
DROP TABLE IF EXISTS src_05345;

CREATE TABLE src_05345 (x UInt64) ENGINE = Null;
CREATE TABLE child_05345 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO child_05345 VALUES (1), (2), (3);
CREATE ROW POLICY OR REPLACE policy_05345 ON child_05345 USING x IN (SELECT x FROM src_05345) TO ALL;
CREATE TABLE merge_05345 (x UInt64) ENGINE = Merge(currentDatabase(), '^child_05345$');
CREATE VIEW v_merge_05345 AS SELECT x FROM merge_05345;
CREATE VIEW v_direct_05345 AS SELECT x FROM child_05345;
CREATE TABLE dst_05345 (x UInt64, via_merge UInt8, via_direct UInt8) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_05345 TO dst_05345 AS
    SELECT x, x IN (SELECT x FROM v_merge_05345) AS via_merge, x IN (SELECT x FROM v_direct_05345) AS via_direct FROM src_05345;

-- Not an asynchronous insert: its flush context has no current database, so the unqualified table of the row policy
-- would be resolved in the default database instead.
INSERT INTO src_05345 SETTINGS async_insert = 0 VALUES (2), (3), (4);
SELECT x, via_merge, via_direct FROM dst_05345 ORDER BY x;

DROP ROW POLICY policy_05345 ON child_05345;
DROP TABLE mv_05345;
DROP TABLE dst_05345;
DROP VIEW v_merge_05345;
DROP VIEW v_direct_05345;
DROP TABLE merge_05345;
DROP TABLE child_05345;
DROP TABLE src_05345;

DROP TABLE IF EXISTS mv_scalar_05345;
DROP TABLE IF EXISTS dst_scalar_05345;
DROP VIEW IF EXISTS v_merge_scalar_05345;
DROP TABLE IF EXISTS merge_scalar_05345;
DROP TABLE IF EXISTS child_scalar_05345;
DROP TABLE IF EXISTS src_scalar_05345;

CREATE TABLE src_scalar_05345 (x UInt64) ENGINE = Null;
CREATE TABLE child_scalar_05345 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO child_scalar_05345 VALUES (1), (2), (3), (4);
CREATE ROW POLICY OR REPLACE policy_scalar_05345 ON child_scalar_05345 USING x <= (SELECT max(x) FROM src_scalar_05345) TO ALL;
CREATE TABLE merge_scalar_05345 (x UInt64) ENGINE = Merge(currentDatabase(), '^child_scalar_05345$');
CREATE VIEW v_merge_scalar_05345 AS SELECT x FROM merge_scalar_05345;
CREATE TABLE dst_scalar_05345 (x UInt64, via_merge UInt8, via_merge_again UInt8) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_scalar_05345 TO dst_scalar_05345 AS
    SELECT x, x IN (SELECT x FROM v_merge_scalar_05345) AS via_merge, x IN (SELECT x FROM v_merge_scalar_05345) AS via_merge_again FROM src_scalar_05345;

INSERT INTO src_scalar_05345 SETTINGS async_insert = 0 VALUES (2), (3), (4);
SELECT x, via_merge, via_merge_again FROM dst_scalar_05345 ORDER BY x;

DROP ROW POLICY policy_scalar_05345 ON child_scalar_05345;
DROP TABLE mv_scalar_05345;
DROP TABLE dst_scalar_05345;
DROP VIEW v_merge_scalar_05345;
DROP TABLE merge_scalar_05345;
DROP TABLE child_scalar_05345;
DROP TABLE src_scalar_05345;
