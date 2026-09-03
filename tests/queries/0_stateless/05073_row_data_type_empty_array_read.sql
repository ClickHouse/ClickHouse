SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_empty_array;

-- A part where every Array(Row) value is empty asks the Row reader for zero rows, which must read nothing.
CREATE TABLE row_empty_array (a UInt64, ar Array(Row(x UInt64, y String)), s String) ENGINE = MergeTree ORDER BY a;
INSERT INTO row_empty_array SELECT number, [], 'str' FROM numbers(10);
INSERT INTO row_empty_array VALUES (100, [(1, 'a')], 'q');

SELECT count(), sum(length(ar)) FROM row_empty_array;
SELECT a, ar, s FROM row_empty_array WHERE a < 3 OR a = 100 ORDER BY a;
OPTIMIZE TABLE row_empty_array FINAL;
SELECT count(), sum(length(ar)), count(DISTINCT s) FROM row_empty_array;

SELECT [[]]::Array(Array(Row(x UInt64, y String))) AS v, toTypeName(v);
SELECT groupArray(ar) FROM row_empty_array WHERE a = 3;

DROP TABLE row_empty_array;
