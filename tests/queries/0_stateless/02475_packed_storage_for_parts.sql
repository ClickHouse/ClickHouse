-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_full;
DROP TABLE IF EXISTS t_packed;

CREATE TABLE t_full (id UInt32, s String, arr Array(LowCardinality(String)))
ENGINE = MergeTree ORDER BY id PARTITION BY id % 4
SETTINGS index_granularity = 64,
min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

CREATE TABLE t_packed (id UInt32, s String, arr Array(LowCardinality(String)))
ENGINE = MergeTree ORDER BY id PARTITION BY id % 4
SETTINGS index_granularity = 64, min_bytes_for_full_part_storage = '100G';

SYSTEM STOP MERGES t_packed;

INSERT INTO t_packed
SELECT
    rand() % 100,
    randomPrintableASCII(rand() % 20),
    arrayMap(x -> toString(x), range(rand() % 10))
FROM numbers(10000);

INSERT INTO t_packed
SELECT
    rand() % 100,
    randomPrintableASCII(rand() % 20),
    arrayMap(x -> toString(x), range(rand() % 10))
FROM numbers(10000);

INSERT INTO t_full SELECT * FROM t_packed;
OPTIMIZE TABLE t_full FINAL;

SELECT table, part_type, part_storage_type, count() FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_full', 't_packed') AND active
GROUP BY table, part_type, part_storage_type ORDER BY table, part_type, part_storage_type;

SELECT (SELECT sum(cityHash64(*)) FROM t_full) = (SELECT sum(cityHash64(*)) FROM t_packed);
SELECT (SELECT sum(cityHash64(*)) FROM t_full WHERE id = 0 or id = 40 or id = 91) = (SELECT sum(cityHash64(*)) FROM t_packed WHERE id = 0 or id = 40 or id = 91);

SYSTEM START MERGES t_packed;
OPTIMIZE TABLE t_packed FINAL;

SELECT (SELECT sum(cityHash64(*)) FROM t_full) = (SELECT sum(cityHash64(*)) FROM t_packed);
SELECT (SELECT sum(cityHash64(*)) FROM t_full WHERE id = 0 or id = 40 or id = 91) = (SELECT sum(cityHash64(*)) FROM t_packed WHERE id = 0 or id = 40 or id = 91);

DROP TABLE IF EXISTS t_full;
DROP TABLE IF EXISTS t_packed;
