DROP TABLE IF EXISTS tab_00481;
CREATE TABLE tab_00481 (date Date, value UInt64, s String, m FixedString(16)) ENGINE = MergeTree(date, (date, value), 8);
INSERT INTO tab_00481 SELECT today() as date, number as value, '' as s, toFixedString('', 16) as m from system.numbers limit 42;
SET preferred_max_column_in_block_size_bytes = 32;
SELECT blockSize(), * from tab_00481 format Null;
SELECT 0;

