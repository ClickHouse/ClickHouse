-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database, no-shared-merge-tree: the test creates two explicit replicas
-- of one ReplicatedMergeTree table to check that replicated fetches ignore the size limits.

-- Test for the max_table_size_rows setting with ReplicatedMergeTree.

DROP TABLE IF EXISTS t_max_size_r1 SYNC;
DROP TABLE IF EXISTS t_max_size_r2 SYNC;

CREATE TABLE t_max_size_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_max_size', 'r1') ORDER BY x SETTINGS max_table_size_rows = 10;
CREATE TABLE t_max_size_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_max_size', 'r2') ORDER BY x SETTINGS max_table_size_rows = 10;

-- The limits are checked against the current table size, so an insert that crosses the limit succeeds.
INSERT INTO t_max_size_r1 SELECT number FROM numbers(8);
INSERT INTO t_max_size_r1 VALUES (8), (9), (10), (11), (12);

-- Replicated fetches are not checked, so the data is replicated despite exceeding the limit.
SYSTEM SYNC REPLICA t_max_size_r2;
SELECT count() FROM t_max_size_r2;

-- Now the table exceeds the limit and inserts into any replica are rejected.
INSERT INTO t_max_size_r1 VALUES (1); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
INSERT INTO t_max_size_r2 VALUES (1); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

-- The data can be removed from a table that exceeds the limit, and then inserts work again.
ALTER TABLE t_max_size_r1 DROP PARTITION tuple();
SYSTEM SYNC REPLICA t_max_size_r2;
INSERT INTO t_max_size_r2 VALUES (1);
SYSTEM SYNC REPLICA t_max_size_r1;
SELECT count() FROM t_max_size_r1;

DROP TABLE t_max_size_r1 SYNC;
DROP TABLE t_max_size_r2 SYNC;
