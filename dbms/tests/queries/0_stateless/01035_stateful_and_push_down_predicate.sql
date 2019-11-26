DROP TABLE IF EXISTS test;

CREATE TABLE test ( A Int32, B Int32 ) ENGINE = Memory();

INSERT INTO test VALUES(1, 2)(0, 3)(1, 4)(0, 5);

SELECT B, neighbor(B, 1) AS next_B FROM (SELECT * FROM test ORDER BY B);

SELECT B, neighbor(B, 1) AS next_B FROM (SELECT * FROM test ORDER BY B) WHERE A == 1;

SELECT B, next_B FROM (SELECT A, B, neighbor(B, 1) AS next_B FROM (SELECT * FROM test ORDER BY B)) WHERE A == 1;


SELECT k, v, d, i FROM (
    SELECT
        t.1 AS k,
        t.2 AS v,
        runningDifference(v) AS d,
        runningDifference(xxHash32(t.1)) AS i
    FROM
    (
        SELECT arrayJoin([('a', 1), ('a', 2), ('a', 3), ('b', 11), ('b', 13), ('b', 15)]) AS t
    )
) WHERE i = 0;

DROP TABLE IF EXISTS test;

