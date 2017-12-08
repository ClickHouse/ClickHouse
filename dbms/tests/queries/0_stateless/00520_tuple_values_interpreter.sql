DROP TABLE IF EXISTS test.tuple;
CREATE TABLE test.tuple (t Tuple(Date, UInt32, UInt64)) ENGINE = Memory;
INSERT INTO test.tuple VALUES ((concat('2000', '-01-01'), /* Hello */ 12+3, 45+6));

SET input_format_values_interpret_expressions = 0;
INSERT INTO test.tuple VALUES (('2000-01-01', 123, 456));

SELECT * FROM test.tuple ORDER BY t;
DROP TABLE test.tuple;
