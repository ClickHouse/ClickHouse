-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_spill_full;
DROP TABLE IF EXISTS t_spill_packed;
DROP TABLE IF EXISTS t_no_spill_packed;

CREATE TABLE t_spill_full (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

-- A tiny max_bytes_to_buffer_for_packed_part makes the packed part writer spill
-- the buffered data to temporary files on almost every write.
CREATE TABLE t_spill_packed (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_full_part_storage = '100G',
         max_bytes_to_buffer_for_packed_part = 10;

-- A large max_bytes_to_buffer_for_packed_part keeps all data in memory (no spill).
CREATE TABLE t_no_spill_packed (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_full_part_storage = '100G',
         max_bytes_to_buffer_for_packed_part = '100M';

INSERT INTO t_spill_packed
SELECT number, randomPrintableASCII(rand() % 20)
FROM numbers(100000);

INSERT INTO t_no_spill_packed SELECT * FROM t_spill_packed;
INSERT INTO t_spill_full SELECT * FROM t_spill_packed;

SELECT (SELECT sum(cityHash64(*)) FROM t_spill_full) = (SELECT sum(cityHash64(*)) FROM t_spill_packed);
SELECT (SELECT count() FROM t_spill_full) = (SELECT count() FROM t_spill_packed);
SELECT count(), uniqExact(id) FROM t_spill_packed;
SELECT (SELECT sum(cityHash64(*)) FROM t_no_spill_packed) = (SELECT sum(cityHash64(*)) FROM t_spill_packed);
SELECT count(), uniqExact(id) FROM t_no_spill_packed;
SELECT part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_spill_packed', 't_no_spill_packed') AND active
ORDER BY table;

DROP TABLE IF EXISTS t_spill_full;
DROP TABLE IF EXISTS t_spill_packed;
DROP TABLE IF EXISTS t_no_spill_packed;
