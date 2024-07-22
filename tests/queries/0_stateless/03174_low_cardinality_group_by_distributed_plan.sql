-- There are various tests that check that group by keys don't propagate into functions replacing const arguments
-- by full (empty) columns

DROP TABLE IF EXISTS dist_03174;
DROP TABLE IF EXISTS set_index_not__fuzz_0;

-- https://github.com/ClickHouse/ClickHouse/issues/63006

SET allow_experimental_analyzer=1;

-- { echoOn }
SELECT concatWithSeparator('.', toUInt128(6), '666' as b, materialize(toLowCardinality(8)))
FROM system.one
GROUP BY '666';

SELECT concatWithSeparator('.', toUInt128(6), '666' as b, materialize(toLowCardinality(8)))
FROM remote('127.0.0.{1,1}', 'system.one')
GROUP BY '666';

-- https://github.com/ClickHouse/ClickHouse/issues/63006
SELECT
    6,
    concat(' World', toUInt128(6), 6, 6, 6, toNullable(6), materialize(toLowCardinality(toNullable(toUInt128(6))))) AS a,
    concat(concat(' World', 6, toLowCardinality(6), ' World', toUInt256(6), materialize(6), 6, toNullable(6), 6, 6, NULL, 6, 6), ' World', 6, 6, 6, 6, toUInt256(6), NULL, 6, 6) AS b
FROM system.one
GROUP BY toNullable(6)
    WITH ROLLUP
WITH TOTALS;

SELECT
    6,
    concat(' World', toUInt128(6), 6, 6, 6, toNullable(6), materialize(toLowCardinality(toNullable(toUInt128(6))))) AS a,
    concat(concat(' World', 6, toLowCardinality(6), ' World', toUInt256(6), materialize(6), 6, toNullable(6), 6, 6, NULL, 6, 6), ' World', 6, 6, 6, 6, toUInt256(6), NULL, 6, 6) AS b
FROM remote('127.0.0.1')
GROUP BY toNullable(6)
    WITH ROLLUP
    WITH TOTALS;

-- https://github.com/ClickHouse/ClickHouse/issues/64945
-- { echoOff }
CREATE TABLE dist_03174 AS system.one ENGINE = Distributed(test_cluster_two_shards, system, one, dummy);

-- { echoOn }
SELECT
    '%',
    tuple(concat('%', 1, toLowCardinality(toLowCardinality(toNullable(materialize(1)))), currentDatabase(), 101., toNullable(13), '%AS%id_02%', toNullable(toNullable(10)), toLowCardinality(toNullable(10)), 10, 10)),
    (toDecimal128(99.67, 6), 36, 61, 14)
FROM dist_03174
WHERE dummy IN (0, '255')
GROUP BY
    toNullable(13),
    (99.67, 61, toLowCardinality(14));

-- Parallel replicas
-- { echoOff }
CREATE TABLE set_index_not__fuzz_0
(
    `name` String,
    `status` Enum8('alive' = 0, 'rip' = 1),
    INDEX idx_status status TYPE set(2) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY name;

INSERT INTO set_index_not__fuzz_0 SELECT * FROM generateRandom() LIMIT 10;

-- { echoOn }
SELECT
    38,
    concat(position(concat(concat(position(concat(toUInt256(3)), 'ca', 2), 3), NULLIF(1, materialize(toLowCardinality(1)))), toLowCardinality(toNullable('ca'))), concat(NULLIF(1, 1), concat(3), toNullable(3)))
FROM set_index_not__fuzz_0
GROUP BY
    toNullable(3),
    concat(concat(CAST(NULL, 'Nullable(Int8)'), toNullable(3)))
FORMAT Null
SETTINGS max_threads = 1, allow_experimental_analyzer = 1, cluster_for_parallel_replicas = 'parallel_replicas', max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_threads = 1;

-- { echoOff }
DROP TABLE IF EXISTS dist_03174;
DROP TABLE IF EXISTS set_index_not__fuzz_0;
