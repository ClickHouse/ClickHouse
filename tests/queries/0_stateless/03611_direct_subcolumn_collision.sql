-- Tags: no-parallel
-- Test case for direct subcolumn collision
-- This tries to trigger the addSubcolumns function multiple times

CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);

-- Create a view that uses subcolumns
CREATE VIEW v0 AS SELECT c0.null FROM t0;

-- Try to access the subcolumn multiple times in different ways
SELECT c0.null FROM t0;
SELECT c0.null FROM v0;
SELECT tx.c0.null FROM t0 tx;
SELECT tx.c0.null FROM t0 tx JOIN t0 ty ON tx.c0 = ty.c0;


-- Cleanup first table and view
DROP VIEW v0;
DROP TABLE t0;

-- Test case from issue: GLOBAL RIGHT JOIN with parallel reading
DROP TABLE IF EXISTS t0_replicated;
CREATE TABLE t0_replicated (c0 Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0_replicated (c0) VALUES (1);
SELECT tx.c0.null FROM t0_replicated tx RIGHT JOIN t0_replicated AS ty ON tx.c0 = ty.c0;
DROP TABLE t0_replicated;

