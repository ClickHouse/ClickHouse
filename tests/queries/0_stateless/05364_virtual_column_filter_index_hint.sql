-- Pruning parts by virtual columns executes the index-analysis copy of the filter; an `indexHint` in it must be executable lazily.

DROP TABLE IF EXISTS t_05364;
CREATE TABLE t_05364 (p UInt8, id UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_05364 SELECT number % 4, number FROM numbers(400);
SET max_rows_to_read = 100; -- only partition 1 may be read

SELECT count(), sum(id) FROM t_05364 WHERE startsWith(_part, '1_') AND notEmpty(_part) = indexHint(intDiv(10, length(_part)) = 1) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count(), sum(id) FROM t_05364 WHERE startsWith(_part, '1_') AND notEmpty(_part) = indexHint(intDiv(10, length(_part)) = 1, 'x') SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count(), sum(id) FROM t_05364 WHERE startsWith(_part, '1_') AND indexHint(toFixedString(_part, 64)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count(), sum(id) FROM t_05364 WHERE startsWith(_part, '1_') AND notEmpty(_part) = indexHint(257) SETTINGS short_circuit_function_evaluation = 'force_enable';

DROP TABLE t_05364;
