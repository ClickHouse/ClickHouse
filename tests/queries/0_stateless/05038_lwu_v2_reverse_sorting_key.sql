-- A lightweight `UPDATE` builds the patch part's sorting key from the table's own sorting key and then
-- appends `_block_number` and `_block_offset` to it. Those two columns must be appended exactly once:
-- appending them to the key expression as well as passing them as additional columns leaves the key
-- description with two more expressions than reverse flags, and a sorting key that carries a direction
-- is the only case where the reverse flags are populated at all, so it is the only case that breaks.

SET enable_lightweight_update = 1;

-- Descending sorting key.

DROP TABLE IF EXISTS t_lwu_desc SYNC;

CREATE TABLE t_lwu_desc (id UInt64, v UInt64)
ENGINE = MergeTree
ORDER BY id DESC
SETTINGS allow_experimental_reverse_key = 1, enable_block_number_column = true, enable_block_offset_column = true;

INSERT INTO t_lwu_desc SELECT number, number FROM numbers(10);
UPDATE t_lwu_desc SET v = v * 100 WHERE id % 3 = 0;
SELECT id, v FROM t_lwu_desc ORDER BY id;

-- Mixed-direction sorting key.

DROP TABLE IF EXISTS t_lwu_mixed SYNC;

CREATE TABLE t_lwu_mixed (a UInt64, b UInt64, v UInt64)
ENGINE = MergeTree
ORDER BY (a, b DESC)
SETTINGS allow_experimental_reverse_key = 1, enable_block_number_column = true, enable_block_offset_column = true;

INSERT INTO t_lwu_mixed SELECT number % 3, number, number FROM numbers(9);
UPDATE t_lwu_mixed SET v = v * 100 WHERE b % 2 = 0;
SELECT a, b, v FROM t_lwu_mixed ORDER BY a, b;

-- Ascending sorting key, which already worked, as a control.

DROP TABLE IF EXISTS t_lwu_asc SYNC;

CREATE TABLE t_lwu_asc (id UInt64, v UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = true, enable_block_offset_column = true;

INSERT INTO t_lwu_asc SELECT number, number FROM numbers(10);
UPDATE t_lwu_asc SET v = v * 100 WHERE id % 3 = 0;
SELECT id, v FROM t_lwu_asc ORDER BY id;

DROP TABLE t_lwu_desc SYNC;
DROP TABLE t_lwu_mixed SYNC;
DROP TABLE t_lwu_asc SYNC;
