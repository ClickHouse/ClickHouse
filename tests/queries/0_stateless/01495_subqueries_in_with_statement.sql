DROP TABLE IF EXISTS test1;

CREATE TABLE test1(i int, j int) ENGINE Log;

INSERT INTO test1 VALUES (1, 2), (3, 4);

WITH test1 AS (SELECT * FROM numbers(5)) SELECT * FROM test1;
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM test1;
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM (SELECT * FROM test1);
SELECT * FROM (WITH test1 AS (SELECT toInt32(*) i FROM numbers(5)) SELECT * FROM test1) l ANY INNER JOIN test1 r on (l.i == r.i);
WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT toInt64(4) i, toInt64(5) j FROM numbers(3) WHERE (i, j) IN test1;

DROP TABLE IF EXISTS test1;
