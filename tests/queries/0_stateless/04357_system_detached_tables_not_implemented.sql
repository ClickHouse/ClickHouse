DROP DATABASE IF EXISTS test_dict_db_04357;
DROP DATABASE IF EXISTS test_ordinary_db_04357;

CREATE DATABASE test_dict_db_04357 ENGINE = Dictionary;
CREATE DATABASE test_ordinary_db_04357 ENGINE = Atomic;

CREATE TABLE test_ordinary_db_04357.t1 (id UInt32) ENGINE = MergeTree ORDER BY id;
DETACH TABLE test_ordinary_db_04357.t1;

SELECT database, table FROM system.detached_tables WHERE database = 'test_ordinary_db_04357';

SELECT COUNT() >= 0 FROM system.detached_tables FORMAT Null;

SELECT 'OK';

DROP DATABASE test_dict_db_04357;
DROP DATABASE test_ordinary_db_04357;
