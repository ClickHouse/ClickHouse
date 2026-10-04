-- Test for the max_table_size_rows, max_table_size_bytes_compressed, max_table_size_bytes_uncompressed settings.

-- Note: an INSERT SELECT can be split into multiple parts (depending on max_insert_threads and block sizes),
-- and the limits are checked on every part commit, so multi-row inserts below keep the table size within
-- the limit, while the inserts crossing the limit use small VALUES lists that always form a single part.

DROP TABLE IF EXISTS t_max_size_rows;

CREATE TABLE t_max_size_rows (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_table_size_rows = 10;

-- The limits are checked against the current table size, so an insert that crosses the limit succeeds.
INSERT INTO t_max_size_rows SELECT number FROM numbers(8);
INSERT INTO t_max_size_rows VALUES (8), (9), (10), (11), (12);
SELECT count() FROM t_max_size_rows;

-- Now the table exceeds the limit and further inserts are rejected.
INSERT INTO t_max_size_rows VALUES (1); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

-- The data can be removed from a table that exceeds the limit, and then inserts work again.
TRUNCATE TABLE t_max_size_rows;
INSERT INTO t_max_size_rows VALUES (1);
SELECT count() FROM t_max_size_rows;

-- The same for DROP PARTITION.
INSERT INTO t_max_size_rows SELECT number FROM numbers(9);
INSERT INTO t_max_size_rows VALUES (100), (101);
INSERT INTO t_max_size_rows VALUES (2); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
ALTER TABLE t_max_size_rows DROP PARTITION tuple();
INSERT INTO t_max_size_rows VALUES (3);
SELECT count() FROM t_max_size_rows;

-- The limit can be changed for an existing table.
INSERT INTO t_max_size_rows SELECT number FROM numbers(9);
INSERT INTO t_max_size_rows VALUES (200), (201);
INSERT INTO t_max_size_rows VALUES (4); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
ALTER TABLE t_max_size_rows MODIFY SETTING max_table_size_rows = 1000;
INSERT INTO t_max_size_rows VALUES (4);
SELECT count() FROM t_max_size_rows;

DROP TABLE t_max_size_rows;

-- The limits on the number of bytes.
DROP TABLE IF EXISTS t_max_size_bytes;

CREATE TABLE t_max_size_bytes (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_table_size_bytes_compressed = 1;

INSERT INTO t_max_size_bytes VALUES (1);
INSERT INTO t_max_size_bytes VALUES (2); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_bytes;

DROP TABLE t_max_size_bytes;

CREATE TABLE t_max_size_bytes (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_table_size_bytes_uncompressed = 1;

INSERT INTO t_max_size_bytes VALUES (1);
INSERT INTO t_max_size_bytes VALUES (2); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_bytes;

DROP TABLE t_max_size_bytes;

-- Inserts by materialized views are checked as well.
DROP TABLE IF EXISTS t_max_size_mv_src;
DROP TABLE IF EXISTS t_max_size_mv_dst;
DROP TABLE IF EXISTS t_max_size_mv;

CREATE TABLE t_max_size_mv_src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_max_size_mv_dst (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_table_size_rows = 10;
CREATE MATERIALIZED VIEW t_max_size_mv TO t_max_size_mv_dst AS SELECT x FROM t_max_size_mv_src;

INSERT INTO t_max_size_mv_src SELECT number FROM numbers(8);
INSERT INTO t_max_size_mv_src VALUES (8), (9), (10), (11);
INSERT INTO t_max_size_mv_src VALUES (1); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_mv_dst;

DROP TABLE t_max_size_mv;
DROP TABLE t_max_size_mv_dst;
DROP TABLE t_max_size_mv_src;
