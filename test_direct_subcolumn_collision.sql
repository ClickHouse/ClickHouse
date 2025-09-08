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

-- Test case from issue: ReplicatedMergeTree with GLOBAL RIGHT JOIN and parallel reading
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t0/alter', 'r1') ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT tx.c0.null FROM t0 tx GLOBAL RIGHT JOIN t0 AS ty ON tx.c0 = ty.c0 SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas';
DROP TABLE t0;

-- Cleanup
DROP VIEW v0;
DROP TABLE t0;

