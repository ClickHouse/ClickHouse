SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC NULLS FIRST;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC NULLS LAST;

SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS FIRST;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS LAST;

SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x ASC, y ASC;
SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x ASC, y ASC NULLS FIRST;
SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS FIRST, y ASC NULLS FIRST;

SET max_block_size = 5;

SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC NULLS FIRST;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC NULLS LAST;

SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS FIRST;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS LAST;

SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x ASC, y ASC;
SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x ASC, y ASC NULLS FIRST;
SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS FIRST, y ASC NULLS FIRST;

SET max_block_size = 3;

SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC NULLS FIRST;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x ASC NULLS LAST;

SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS FIRST;
SELECT x FROM (SELECT number % 5 = 0 ? NULL : (number % 3 = 0 ? nan : toFloat64(number)) AS x FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS LAST;

SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x ASC, y ASC;
SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x ASC, y ASC NULLS FIRST;
SELECT x, y FROM (SELECT number % 5 = 0 ? NULL : number AS x, number % 3 = 0 ? nan : toFloat64(number) AS y FROM system.numbers LIMIT 10) ORDER BY x DESC NULLS FIRST, y ASC NULLS FIRST;
