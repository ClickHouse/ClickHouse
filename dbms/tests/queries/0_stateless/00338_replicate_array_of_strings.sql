DROP TABLE IF EXISTS test.bad_arrays;
CREATE TABLE test.bad_arrays (a Array(String), b Array(UInt8)) ENGINE = Memory;

INSERT INTO test.bad_arrays VALUES ([''],[]),([''],[1]);

SELECT a FROM test.bad_arrays ARRAY JOIN b;

DROP TABLE test.bad_arrays;


DROP TABLE IF EXISTS test.bad_arrays;
CREATE TABLE test.bad_arrays (a Array(String), b Array(String)) ENGINE = Memory;

INSERT INTO test.bad_arrays VALUES ([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),(['abc'],['223750']),(['ноутбук acer aspire e5-532-p3p2'],[]),([''],[]),([''],[]),([''],[]),([''],[]),(['лучшие моноблоки 2016'],[]),(['лучшие моноблоки 2016'],[]),([''],[]),([''],[]);

SELECT a FROM test.bad_arrays ARRAY JOIN b;

DROP TABLE test.bad_arrays;
