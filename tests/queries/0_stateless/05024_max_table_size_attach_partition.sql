-- The parts of an attached partition are committed in a single transaction, one by one, under the same
-- lock on the parts set. The whole batch is counted against the 'max_table_size_*' limits before any of
-- its parts is accepted into the working set, so the batch cannot overshoot the limits.

DROP TABLE IF EXISTS t_max_size_attach_src;
DROP TABLE IF EXISTS t_max_size_attach_dst;

-- Prevent background merges, so that the source table keeps four separate parts.
CREATE TABLE t_max_size_attach_src (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_max_size_attach_dst (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS max_table_size_rows = 10, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_max_size_attach_src VALUES (1), (2), (3), (4);
INSERT INTO t_max_size_attach_src VALUES (5), (6), (7), (8);
INSERT INTO t_max_size_attach_src VALUES (9), (10), (11), (12);
INSERT INTO t_max_size_attach_src VALUES (13), (14), (15), (16);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_max_size_attach_src' AND active;

ALTER TABLE t_max_size_attach_dst ATTACH PARTITION tuple() FROM t_max_size_attach_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_attach_dst;

DROP TABLE t_max_size_attach_dst;
DROP TABLE t_max_size_attach_src;
