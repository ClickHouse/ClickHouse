-- Tags: no-replicated-database
-- no-replicated-database: with ReplicatedMergeTree, merges are executed by the replication queue
-- in the background, so OPTIMIZE cannot report the error of a failed merge to the client.

-- Committing the results of merges is checked against the table size limits.

DROP TABLE IF EXISTS t_max_size_merge;

-- Prevent regular background merges (by the max size of merged parts) so that the parts
-- are always free to be selected by OPTIMIZE, which ignores this setting.
CREATE TABLE t_max_size_merge (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS max_table_size_rows = 10, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_max_size_merge SELECT number FROM numbers(8);
INSERT INTO t_max_size_merge VALUES (8), (9), (10), (11), (12), (13), (14), (15);
SELECT count() FROM t_max_size_merge;

-- Merges of a table that exceeds the limit are rejected when their results are committed.
OPTIMIZE TABLE t_max_size_merge FINAL; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

-- After the size is reduced below the limit, merges work again.
TRUNCATE TABLE t_max_size_merge;
INSERT INTO t_max_size_merge SELECT number FROM numbers(4);
INSERT INTO t_max_size_merge VALUES (4), (5), (6), (7);
OPTIMIZE TABLE t_max_size_merge FINAL;
SELECT count() FROM t_max_size_merge;

DROP TABLE t_max_size_merge;
