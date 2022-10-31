DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `t` UInt8,
    `flag` UInt8,
    `id` UInt8
)
ENGINE = MergeTree
PARTITION BY t
ORDER BY (t, id)
SETTINGS index_granularity = 8192;

INSERT INTO test VALUES (1,0,1),(1,0,2),(1,0,3),(1,0,4),(1,0,5),(1,0,6),(1,1,7),(0,0,7);

set query_plan_filter_push_down = true;

SELECT id, flag FROM test t1
INNER JOIN  (SELECT DISTINCT id FROM test) AS t2 ON t1.id = t2.id
WHERE flag = 0 and t = 1 AND id NOT IN (SELECT 1 WHERE 0)
ORDER BY id;

DROP TABLE IF EXISTS test;
