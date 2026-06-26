-- Tags: shard

DROP TABLE IF EXISTS test_optimize_subcolumns_local;
DROP TABLE IF EXISTS test_optimize_subcolumns_dist;

CREATE TABLE test_optimize_subcolumns_local
(
    uid Int16,
    name LowCardinality(String),
    age Int16
)
ENGINE = Memory;

CREATE TABLE test_optimize_subcolumns_dist
(
    uid Int16,
    name String,
    age Int16
)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_optimize_subcolumns_local);

INSERT INTO test_optimize_subcolumns_local VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

SELECT age
FROM test_optimize_subcolumns_dist
WHERE name != ''
ORDER BY age
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;

DROP TABLE test_optimize_subcolumns_dist;
DROP TABLE test_optimize_subcolumns_local;
