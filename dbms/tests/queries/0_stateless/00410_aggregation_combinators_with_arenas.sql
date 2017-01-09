DROP TABLE IF EXISTS test.arena;
CREATE TABLE test.arena (k UInt8, d String) ENGINE = Memory;
INSERT INTO test.arena SELECT number % 10 AS k, hex(intDiv(number, 10) % 1000) AS d FROM system.numbers LIMIT 10000000;
SELECT length(groupUniqArrayIf(d, d != hex(0))) FROM test.arena GROUP BY k;
SELECT length(groupUniqArrayMerge(ds)) FROM (SELECT k, groupUniqArrayState(d) AS ds FROM test.arena GROUP BY k) GROUP BY k;
DROP TABLE IF EXISTS test.arena;
