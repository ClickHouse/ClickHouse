-- Tags: replica

DROP TABLE IF EXISTS bad_arrays;
CREATE TABLE bad_arrays (a Array(String), b Array(UInt8)) ENGINE = Memory;

INSERT INTO bad_arrays VALUES ([''],[]),([''],[1]);

SELECT a FROM bad_arrays ARRAY JOIN b;

DROP TABLE bad_arrays;


DROP TABLE IF EXISTS bad_arrays;
CREATE TABLE bad_arrays (a Array(String), b Array(String)) ENGINE = Memory;

INSERT INTO bad_arrays VALUES ([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),(['abc'],['223750']),(['ноутбук acer aspire e5-532-p3p2'],[]),([''],[]),([''],[]),([''],[]),([''],[]),(['лучшие моноблоки 2016'],[]),(['лучшие моноблоки 2016'],[]),([''],[]),([''],[]);

SELECT a FROM bad_arrays ARRAY JOIN b;

DROP TABLE bad_arrays;


DROP TABLE IF EXISTS bad_arrays;
CREATE TABLE bad_arrays (a Array(String), b Array(UInt8)) ENGINE = Memory;

INSERT INTO bad_arrays VALUES (['abc','def'],[1,2,3]),([],[1,2]),(['a','b'],[]),(['Hello'],[1,2]),([],[]),(['x','y','z'],[4,5,6]);

SELECT a, b FROM bad_arrays ARRAY JOIN b;

DROP TABLE bad_arrays;
