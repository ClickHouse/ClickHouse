-- Tags: no-replicated-database
-- no-replicated-database: with ReplicatedMergeTree, merges are executed by the replication queue
-- in the background, so OPTIMIZE cannot report the error of a failed merge to the client.

-- Committing the results of merges and mutations is checked against the table size limits, but an
-- operation that replaces the covered parts with a not larger part does not increase the size of the
-- table, so it is allowed even when the table already exceeds the limits. Otherwise a table over the
-- limit could be brought back under it only by dropping whole parts.

DROP TABLE IF EXISTS t_max_size_merge;

-- Prevent regular background merges (by the max size of merged parts) so that the parts
-- are always free to be selected by OPTIMIZE, which ignores this setting.
CREATE TABLE t_max_size_merge (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS max_table_size_rows = 10, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_max_size_merge SELECT number FROM numbers(8);
INSERT INTO t_max_size_merge VALUES (8), (9), (10), (11), (12), (13), (14), (15);
SELECT count() FROM t_max_size_merge;

-- The table exceeds the limit, so new data is rejected.
INSERT INTO t_max_size_merge VALUES (100); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

-- A merge does not add rows, so it is allowed.
OPTIMIZE TABLE t_max_size_merge FINAL;
SELECT count() FROM t_max_size_merge;

-- A mutation that removes rows is allowed as well, and it brings the table back under the limit.
ALTER TABLE t_max_size_merge DELETE WHERE x >= 6 SETTINGS mutations_sync = 2;
SELECT count() FROM t_max_size_merge;

INSERT INTO t_max_size_merge VALUES (100);
SELECT count() FROM t_max_size_merge;

DROP TABLE t_max_size_merge;
