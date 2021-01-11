DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1 (a UInt8, b Array(DateTime)) ENGINE Memory;
CREATE TABLE test2 as test1 ENGINE Distributed(test_shard_localhost, currentDatabase(), test1);

INSERT INTO test1 VALUES (1, [1, 2, 3]);

SELECT 1
FROM test2 AS test2
ARRAY JOIN arrayFilter(t -> (t GLOBAL IN
    (
        SELECT DISTINCT now() AS `ym:a`
        WHERE 1
    )), test2.b) AS test2_b
WHERE 1;

DROP TABLE test1;
DROP TABLE test2;
