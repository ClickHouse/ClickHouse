DROP TABLE IF EXISTS test_map;
CREATE TABLE test_map (value Map(String, String)) ENGINE=TinyLog;

SELECT 'Non constant map';
INSERT INTO test_map VALUES ({'K0':'V0'});
SELECT has(value, 'K0') FROM test_map;
SELECT has(value, 'K1') FROM test_map;

SELECT 'Constant map';

SELECT has(map('K0', 'V0'), 'K0') FROM system.one;
SELECT has(map('K0', 'V0'), 'K1') FROM system.one;

DROP TABLE test_map;
