DROP TABLE IF EXISTS test.group_uniq;
CREATE TABLE test.group_uniq ENGINE = Memory AS SELECT number % 10 as id, toString(intDiv((number%10000), 10)) as v FROM system.numbers LIMIT 10000000;

INSERT INTO test.group_uniq SELECT 2 as id, toString(number % 100) as v FROM system.numbers LIMIT 1000000;
INSERT INTO test.group_uniq SELECT 5 as id, toString(number % 100) as v FROM system.numbers LIMIT 10000000;

SELECT length(groupUniqArray(v)) FROM test.group_uniq GROUP BY id ORDER BY id;
--SELECT length(groupUniqArray(v)) FROM remote('127.0.0.{1,2,3,4}', 'test', 'group_uniq') GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS test.group_uniq;