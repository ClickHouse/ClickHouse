-- Tags: no-replicated-database
-- no-replicated-database: the test asserts how much of a partially rejected insert survives, and with
-- the Replicated database engine the parts are committed through the ReplicatedMergeTree sink instead.

-- An insert that writes several parts is checked when each of them is committed: the parts written
-- before 'max_table_size_rows' was crossed stay in the table and the rest of the insert is rejected.

DROP TABLE IF EXISTS t_max_size_rows_multiple_parts;

CREATE TABLE t_max_size_rows_multiple_parts (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
    SETTINGS max_table_size_rows = 3;

-- One part per partition: the first one is written into a table that is still within the limit, the
-- second one is over it and does not replace anything, so it is rejected.
INSERT INTO t_max_size_rows_multiple_parts VALUES (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

SELECT count() FROM t_max_size_rows_multiple_parts;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_max_size_rows_multiple_parts' AND active;

DROP TABLE t_max_size_rows_multiple_parts;
