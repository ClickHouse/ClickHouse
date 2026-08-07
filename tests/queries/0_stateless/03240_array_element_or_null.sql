-- { echoOn }
DROP TABLE IF EXISTS array_element_or_null_test;
CREATE TABLE array_element_or_null_test (arr Array(Int32), id Int32) ENGINE = Memory;
insert into array_element_or_null_test VALUES ([11,12,13], 2), ([11,12], 3), ([11,12,13], -1), ([11,12], -2), ([11,12], -3), ([11], 0);
select arrayElementOrNull(arr, id) from array_element_or_null_test;

DROP TABLE IF EXISTS array_element_or_null_test;
CREATE TABLE array_element_or_null_test (arr Array(Int32), id UInt32) ENGINE = Memory;
insert into array_element_or_null_test VALUES ([11,12,13], 2), ([11,12], 3), ([11,12,13], 1), ([11,12], 4), ([11], 0);
select arrayElementOrNull(arr, id) from array_element_or_null_test;

DROP TABLE IF EXISTS array_element_or_null_test;
CREATE TABLE array_element_or_null_test (arr Array(String), id Int32) ENGINE = Memory;
insert into array_element_or_null_test VALUES (['Abc','Df','Q'], 2), (['Abc','DEFQ'], 3), (['ABC','Q','ERT'], -1), (['Ab','ber'], -2), (['AB','asd'], -3), (['A'], 0);
select arrayElementOrNull(arr, id) from array_element_or_null_test;

DROP TABLE IF EXISTS array_element_or_null_test;
CREATE TABLE array_element_or_null_test (arr Array(String), id UInt32) ENGINE = Memory;
insert into array_element_or_null_test VALUES (['Abc','Df','Q'], 2), (['Abc','DEFQ'], 3), (['ABC','Q','ERT'], 1), (['Ab','ber'], 4), (['A'], 0);
select arrayElementOrNull(arr, id) from array_element_or_null_test;

DROP TABLE IF EXISTS array_element_or_null_test;
CREATE TABLE array_element_or_null_test (id UInt32) ENGINE = Memory;
insert into array_element_or_null_test VALUES (2), (1), (4), (3), (0);
select [1, 2, 3] as arr, arrayElementOrNull(arr, id) from array_element_or_null_test;

DROP TABLE IF EXISTS array_element_or_null_test;
CREATE TABLE array_element_or_null_test (id Int32) ENGINE = Memory;
insert into array_element_or_null_test VALUES (-2), (1), (-4), (3), (2), (-1), (4), (-3), (0);
select [1, 2, 3] as arr, arrayElementOrNull(arr, id) from array_element_or_null_test;

DROP TABLE array_element_or_null_test;

SELECT arrayElementOrNull(range(0), -1);
SELECT arrayElementOrNull(range(0), 1);
SELECT arrayElementOrNull(range(number), 2) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(range(number), -1) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(range(number), number) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(range(number), 2 - number) FROM system.numbers LIMIT 3;

SELECT arrayElementOrNull(arrayMap(x -> toString(x), range(number)), 2) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(arrayMap(x -> toString(x), range(number)), -1) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(arrayMap(x -> toString(x), range(number)), number) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(arrayMap(x -> toString(x), range(number)), 2 - number) FROM system.numbers LIMIT 3;

SELECT arrayElementOrNull(arrayMap(x -> range(x), range(number)), 2) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(arrayMap(x -> range(x), range(number)), -1) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(arrayMap(x -> range(x), range(number)), number) FROM system.numbers LIMIT 3;
SELECT arrayElementOrNull(arrayMap(x -> range(x), range(number)), 2 - number) FROM system.numbers LIMIT 3;

SELECT arrayElementOrNull([[1]], 1), arrayElementOrNull(materialize([[1]]), 1), arrayElementOrNull([[1]], materialize(1)), arrayElementOrNull(materialize([[1]]), materialize(1));
SELECT arrayElementOrNull([['Hello']], 1), arrayElementOrNull(materialize([['World']]), 1), arrayElementOrNull([['Hello']], materialize(1)), arrayElementOrNull(materialize([['World']]), materialize(1));

SELECT arrayElementOrNull(([[['a'], ['b', 'c']], [['d', 'e', 'f'], ['g', 'h', 'i', 'j'], ['k', 'l', 'm', 'n', 'o']], [['p', 'q', 'r', 's', 't', 'u'], ['v', 'w', 'x', 'y', 'z', 'aa', 'bb'], ['cc', 'dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'jj'], ['kk', 'll', 'mm', 'nn', 'oo', 'pp', 'qq', 'rr', 'ss']]] AS arr), number), arrayElementOrNull(arr[number], number), arrayElementOrNull(arr[number][number], number) FROM system.numbers LIMIT 10;

SELECT arrayElementOrNull([1, 2], 3), arrayElementOrNull([1, NULL, 2], 4), arrayElementOrNull([('1', 1), ('2', 2)], -3);

select groupArray(a) as b, arrayElementOrNull(b, 1), arrayElementOrNull(b, 0) from (select (1, 2) as a);

SELECT [toNullable(1)] AS x, arrayElementOrNull(x, toNullable(1)) AS y;
SELECT materialize([toNullable(1)]) AS x, arrayElementOrNull(x, toNullable(1)) AS y;
SELECT [toNullable(1)] AS x, arrayElementOrNull(x, materialize(toNullable(1))) AS y;
SELECT materialize([toNullable(1)]) AS x, arrayElementOrNull(x, materialize(toNullable(1))) AS y;

select arrayElementOrNull(m, 0), materialize(map('key', 42)) as m; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- { echoOff }
