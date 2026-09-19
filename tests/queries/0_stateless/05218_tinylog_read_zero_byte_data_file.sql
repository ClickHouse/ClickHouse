-- Tags: log-engine
-- log-engine: the defect only exists in the Log family, so replacing the engine with MergeTree makes the test vacuous.

DROP TABLE IF EXISTS t_zero_byte_first;
DROP TABLE IF EXISTS t_exhausted_first;
DROP TABLE IF EXISTS t_zero_byte_later;
DROP TABLE IF EXISTS t_zero_byte_map;

-- An array of only empty arrays writes no elements at all, so `b.bin` is empty while `b.size0.bin` holds
-- the 10 rows; `b.bin` is also the first data file by name.
CREATE TABLE t_zero_byte_first (id Int64, g Int64, b Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_zero_byte_first (id) SELECT number FROM numbers(10);
SELECT count(), sum(id), sum(length(b)) FROM t_zero_byte_first WHERE NOT ignore(*) SETTINGS max_block_size = 3;
SELECT count() FROM t_zero_byte_first WHERE NOT ignore(b) SETTINGS max_block_size = 3;

-- The elements file is not empty here, it just runs out first: only the leading rows hold elements,
-- so `b.bin` is at its end once row 3 has been read while `b.size0.bin` still holds rows 4 to 10.
CREATE TABLE t_exhausted_first (id Int64, g Int64, b Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_exhausted_first SELECT number, number, if(number < 3, [number], []) FROM numbers(10);
SELECT count(), sum(id), sum(length(b)) FROM t_exhausted_first WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- Control: the same types, named so that a file with one entry per row comes first.
CREATE TABLE t_zero_byte_later (id Int64, b Int64, g Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_zero_byte_later (id) SELECT number FROM numbers(10);
SELECT count(), sum(id) FROM t_zero_byte_later WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- A map of only empty maps writes neither keys nor values, and `a%2Ekeys.bin` also comes first by name.
CREATE TABLE t_zero_byte_map (id Int64, a Map(String, UInt64)) ENGINE = TinyLog;
INSERT INTO t_zero_byte_map (id) SELECT number FROM numbers(10);
SELECT count(), sum(id) FROM t_zero_byte_map WHERE NOT ignore(*) SETTINGS max_block_size = 3;

DROP TABLE t_zero_byte_first;
DROP TABLE t_exhausted_first;
DROP TABLE t_zero_byte_later;
DROP TABLE t_zero_byte_map;
