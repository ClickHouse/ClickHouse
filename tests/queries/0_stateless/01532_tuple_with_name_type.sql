DROP TABLE IF EXISTS test_01532_1;
DROP TABLE IF EXISTS test_01532_2;
DROP TABLE IF EXISTS test_01532_3;

CREATE TABLE test_01532_1 (a Tuple(key String, value String)) ENGINE Memory();
DESCRIBE TABLE test_01532_1;

CREATE TABLE test_01532_2 (a Tuple(Tuple(key String, value String))) ENGINE Memory();
DESCRIBE TABLE test_01532_2;

CREATE TABLE test_01532_3 (a Array(Tuple(key String, value String))) ENGINE Memory();
DESCRIBE TABLE test_01532_3;

DROP TABLE test_01532_1;
DROP TABLE test_01532_2;
DROP TABLE test_01532_3;
