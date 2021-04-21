CREATE TABLE numbers_10000 ENGINE = Memory()
    AS SELECT number FROM system.numbers LIMIT 10000;

CREATE TABLE test1_00089 ENGINE = AggregatingMemory() 
    AS SELECT arrayMap(x -> x % 2, groupArray(number)) AS arr FROM numbers_10000 GROUP BY number % ((number * 0xABCDEF0123456789 % 1234) + 1);
INSERT INTO test1_00089 SELECT * FROM numbers_10000;

CREATE TABLE test2_00089 ENGINE = AggregatingMemory()
    AS SELECT arr, count() AS c FROM test1_00089 GROUP BY arr;
INSERT INTO test2_00089 SELECT * FROM test1_00089;

SELECT * FROM test2_00089 ORDER BY c DESC, arr ASC;
