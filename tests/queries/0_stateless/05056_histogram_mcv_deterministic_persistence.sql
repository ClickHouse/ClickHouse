-- Tags: no-fasttest
-- no-fasttest: histogram statistics use the DataSketches KLL and frequent items libraries

-- Same as 05054_histogram_deterministic_persistence, but with skewed data, so that the most common values
-- are evicted and re-admitted while building and merging, and evicted weight is spilled into the residual KLL sketch.

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS histogram_mcv_determinism_1;
DROP TABLE IF EXISTS histogram_mcv_determinism_2;

CREATE TABLE histogram_mcv_determinism_1
(
    id UInt64,
    small_head Float64 STATISTICS(histogram(64, 16)),
    long_tail UInt64 STATISTICS(histogram(128, 768))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

CREATE TABLE histogram_mcv_determinism_2
(
    id UInt64,
    small_head Float64 STATISTICS(histogram(64, 16)),
    long_tail UInt64 STATISTICS(histogram(128, 768))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

-- Both tables must merge the same parts in one merge.
SYSTEM STOP MERGES histogram_mcv_determinism_1;
SYSTEM STOP MERGES histogram_mcv_determinism_2;

-- Zipf-like: value v is drawn with probability ~ 1 / v^2.
INSERT INTO histogram_mcv_determinism_1
SELECT number, intDiv(1000, 1 + cityHash64(number) % 1000) * 0.5, intDiv(1000000, 1 + cityHash64(number, 1) % 1000000)
FROM numbers(30000);
INSERT INTO histogram_mcv_determinism_1
SELECT number + 30000, intDiv(1000, 1 + cityHash64(number, 2) % 1000) - 3.25, intDiv(10000000, 1 + cityHash64(number, 3) % 10000000)
FROM numbers(20000);
INSERT INTO histogram_mcv_determinism_1
SELECT number + 50000, if(number % 7 = 0, -0., number % 13), number % 3000
FROM numbers(10000);

INSERT INTO histogram_mcv_determinism_2
SELECT number, intDiv(1000, 1 + cityHash64(number) % 1000) * 0.5, intDiv(1000000, 1 + cityHash64(number, 1) % 1000000)
FROM numbers(30000);
INSERT INTO histogram_mcv_determinism_2
SELECT number + 30000, intDiv(1000, 1 + cityHash64(number, 2) % 1000) - 3.25, intDiv(10000000, 1 + cityHash64(number, 3) % 10000000)
FROM numbers(20000);
INSERT INTO histogram_mcv_determinism_2
SELECT number + 50000, if(number % 7 = 0, -0., number % 13), number % 3000
FROM numbers(10000);

SYSTEM START MERGES histogram_mcv_determinism_1;
SYSTEM START MERGES histogram_mcv_determinism_2;
OPTIMIZE TABLE histogram_mcv_determinism_1 FINAL;
OPTIMIZE TABLE histogram_mcv_determinism_2 FINAL;

SELECT first.hash_of_all_files = second.hash_of_all_files
FROM
(
    SELECT hash_of_all_files
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'histogram_mcv_determinism_1' AND active
) AS first
CROSS JOIN
(
    SELECT hash_of_all_files
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'histogram_mcv_determinism_2' AND active
) AS second;

-- Rematerializing the statistics of the merged part reproduces the same bytes.
ALTER TABLE histogram_mcv_determinism_1 MATERIALIZE STATISTICS small_head, long_tail SETTINGS mutations_sync = 2;
ALTER TABLE histogram_mcv_determinism_2 MATERIALIZE STATISTICS small_head, long_tail SETTINGS mutations_sync = 2;

SELECT first.hash_of_all_files = second.hash_of_all_files
FROM
(
    SELECT hash_of_all_files
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'histogram_mcv_determinism_1' AND active
) AS first
CROSS JOIN
(
    SELECT hash_of_all_files
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'histogram_mcv_determinism_2' AND active
) AS second;

DROP TABLE histogram_mcv_determinism_1;
DROP TABLE histogram_mcv_determinism_2;
