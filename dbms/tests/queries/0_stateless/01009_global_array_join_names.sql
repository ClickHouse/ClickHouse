DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1 (a UInt8, b Array(DateTime)) ENGINE Memory;
CREATE TABLE test2 as test1 ENGINE Distributed(test_shard, currentDatabase(), test1);

INSERT INTO test1 VALUES (1, [1, 2, 3]);

SELECT 1
FROM d1 AS d1
ARRAY JOIN arrayFilter(t -> (t GLOBAL IN
    (
        SELECT DISTINCT now() AS `ym:a`
        WHERE 1
    )), d1.a) AS d1_a
WHERE 1;

DROP TABLE test1;
DROP TABLE test2;
