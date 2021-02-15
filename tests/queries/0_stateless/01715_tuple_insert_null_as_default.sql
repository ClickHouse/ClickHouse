DROP TABLE IF EXISTS test_tuple;
CREATE TABLE test_tuple (value Tuple(UInt8, UInt8)) ENGINE=TinyLog;

SET input_format_null_as_default = 1;
INSERT INTO test_tuple VALUES ((NULL, 1));
SELECT * FROM test_tuple;

SET input_format_null_as_default = 0;
INSERT INTO test_tuple VALUES ((NULL, 2)); -- { clientError 53 }
SELECT * FROM test_tuple;

DROP TABLE test_tuple;
