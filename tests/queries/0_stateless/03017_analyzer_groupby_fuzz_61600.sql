CREATE TABLE set_index_not__fuzz_0 (`name` String, `status` Enum8('alive' = 0, 'rip' = 1), INDEX idx_status status TYPE set(2) GRANULARITY 1)
ENGINE = MergeTree ORDER BY name
SETTINGS index_granularity = 8192;

INSERT INTO set_index_not__fuzz_0 SELECT * from generateRandom() limit 1;

SELECT
    38,
    concat(position(concat(concat(position(concat(toUInt256(3)), 'ca', 2), 3),NULLIF(1, materialize(toLowCardinality(1)))), toLowCardinality(toNullable('ca'))), concat(NULLIF(1, 1), concat(3), toNullable(3)))
FROM set_index_not__fuzz_0
GROUP BY
    toNullable(3),
    concat(concat(NULLIF(1, 1), toNullable(toNullable(3))))
SETTINGS enable_analyzer = 1;

-- WITH ROLLUP (note that result is different with the analyzer (analyzer is correct including all combinations)
SELECT
    38,
    concat(position(concat(concat(position(concat(toUInt256(3)), 'ca', 2), 3), NULLIF(1, materialize(toLowCardinality(1)))), toLowCardinality(toNullable('ca'))), concat(NULLIF(1, 1), concat(3), toNullable(3)))
FROM set_index_not__fuzz_0
GROUP BY
    toNullable(3),
    concat(concat(NULLIF(1, 1), toNullable(toNullable(3))))
WITH ROLLUP
SETTINGS enable_analyzer = 1;
