-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01676 SYNC;

CREATE DATABASE test_01676;

CREATE TABLE test_01676.dict_data (key UInt64, value UInt64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test_01676.dict_data VALUES (2,20), (3,30), (4,40), (5,50);

CREATE DICTIONARY test_01676.dict (key UInt64, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(DB 'test_01676' TABLE 'dict_data' HOST '127.0.0.1' PORT tcpPort())) LIFETIME(0) LAYOUT(HASHED());

CREATE TABLE test_01676.table (x UInt64, y UInt64 DEFAULT dictGet('test_01676.dict', 'value', x)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test_01676.table (x) VALUES (2);
INSERT INTO test_01676.table VALUES (toUInt64(3), toUInt64(15));

SELECT * FROM test_01676.table ORDER BY x;

SELECT 'status:';
SELECT status FROM system.dictionaries WHERE database='test_01676' AND name='dict';

DETACH DATABASE test_01676;
ATTACH DATABASE test_01676;

SELECT 'status_after_detach_and_attach:';
-- It can be not loaded, or not even finish attaching in case of asynchronous tables loading.
SELECT COALESCE((SELECT status FROM system.dictionaries WHERE database='test_01676' AND name='dict')::Nullable(String), 'NOT_LOADED');

INSERT INTO test_01676.table (x) VALUES (toInt64(4));
SELECT * FROM test_01676.table ORDER BY x;

SELECT 'status:';
SELECT status FROM system.dictionaries WHERE database='test_01676' AND name='dict';

DROP DATABASE test_01676;
