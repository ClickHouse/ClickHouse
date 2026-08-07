SELECT x FROM (SELECT toNullable(1) AS x) WHERE x;
SELECT x FROM (SELECT toNullable(0) AS x) WHERE x;
SELECT x FROM (SELECT NULL AS x) WHERE x;

SELECT 1 WHERE toNullable(1);
SELECT 1 WHERE toNullable(0);
SELECT 1 WHERE NULL;

SELECT x FROM (SELECT toNullable(materialize(1)) AS x) WHERE x;
SELECT x FROM (SELECT toNullable(materialize(0)) AS x) WHERE x;
SELECT x FROM (SELECT materialize(NULL) AS x) WHERE x;

SELECT materialize('Hello') WHERE toNullable(materialize(1));
SELECT materialize('Hello') WHERE toNullable(materialize(0));
SELECT materialize('Hello') WHERE materialize(NULL);

SELECT x, y FROM (SELECT number % 3 = 0 ? NULL : number AS x, number AS y FROM system.numbers LIMIT 10) WHERE x % 2 != 0;
