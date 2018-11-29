DROP TABLE IF EXISTS test.has_function;

CREATE TABLE test.has_function(arr Array(Nullable(String))) ENGINE = Memory;
INSERT INTO test.has_function(arr) values ([null, 'str1', 'str2']),(['str1', 'str2']), ([]), ([]);

SELECT arr, has(`arr`, 'str1') FROM test.has_function;
SELECT has([null, 'str1', 'str2'], 'str1');

DROP TABLE test.has_function;
