-- Test for "Block structure mismatch in UnionStep" bug
-- When liftUpUnion optimization pushes Expression through Union,
-- branches with different headers (due to projection vs non-projection reads)
-- could produce different output structures.

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (i Int32) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t0 SELECT number FROM numbers(1);
ALTER TABLE t0 ADD PROJECTION x (SELECT i ORDER BY i) SETTINGS mutations_sync = 2;
INSERT INTO t0 SELECT number FROM numbers(1);

SELECT 1 FROM t0 WHERE materialize(1) SETTINGS force_optimize_projection = 1;

DROP TABLE t0;
