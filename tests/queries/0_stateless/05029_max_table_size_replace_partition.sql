-- `REPLACE PARTITION FROM` commits the new parts first and removes the destination partition only afterwards,
-- so the parts being replaced are not covered by any of the new parts. The 'max_table_size_*' limits are checked
-- for the operation as a whole, and a replacement that is not larger than the partition it replaces is allowed
-- even when the table has already crossed a limit, exactly like a size-reducing merge or mutation.

DROP TABLE IF EXISTS t_max_size_replace_src;
DROP TABLE IF EXISTS t_max_size_replace_dst;

CREATE TABLE t_max_size_replace_src (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;

CREATE TABLE t_max_size_replace_dst (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
    SETTINGS max_table_size_rows = 10;

-- The limits are checked against the current table size, so the insert crossing the limit succeeds
-- and the table ends up above the limit.
INSERT INTO t_max_size_replace_dst SELECT 1, number FROM numbers(20);
INSERT INTO t_max_size_replace_dst VALUES (1, 100); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_replace_dst;

-- A smaller partition can replace a larger one even though the table is above the limit.
INSERT INTO t_max_size_replace_src SELECT 1, number FROM numbers(5);
ALTER TABLE t_max_size_replace_dst REPLACE PARTITION 1 FROM t_max_size_replace_src;
SELECT count() FROM t_max_size_replace_dst;

-- A replacement that puts the table above the limit is rejected as a whole.
TRUNCATE TABLE t_max_size_replace_src;
INSERT INTO t_max_size_replace_src SELECT 1, number FROM numbers(50);
ALTER TABLE t_max_size_replace_dst REPLACE PARTITION 1 FROM t_max_size_replace_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_replace_dst;

-- The same for `ATTACH PARTITION FROM`, which does not remove anything from the destination table.
ALTER TABLE t_max_size_replace_dst ATTACH PARTITION 1 FROM t_max_size_replace_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_replace_dst;

DROP TABLE t_max_size_replace_dst;
DROP TABLE t_max_size_replace_src;

-- The same for the limits on the number of bytes.
CREATE TABLE t_max_size_replace_src (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;

CREATE TABLE t_max_size_replace_dst (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
    SETTINGS max_table_size_bytes_compressed = 1024;

INSERT INTO t_max_size_replace_dst SELECT 1, number FROM numbers(100000);
INSERT INTO t_max_size_replace_dst VALUES (1, 100); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_replace_dst;

-- A partition of a single row is smaller than the partition it replaces, so the replacement is allowed.
INSERT INTO t_max_size_replace_src VALUES (1, 1);
ALTER TABLE t_max_size_replace_dst REPLACE PARTITION 1 FROM t_max_size_replace_src;
SELECT count() FROM t_max_size_replace_dst;

DROP TABLE t_max_size_replace_dst;
DROP TABLE t_max_size_replace_src;
