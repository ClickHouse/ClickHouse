-- `use_skip_indexes_if_final_exact_mode` must add back exactly the granules whose primary key
-- interval intersects the primary key interval of some granule selected by the skip index.
-- The selected granules are scattered over parts with overlapping key ranges.

SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;
SET query_plan_optimize_lazy_final = 0;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
-- Keep the rows with equal keys of an insert, otherwise `ReplacingMergeTree` merges them and the parts change.
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_scattered;

CREATE TABLE t_scattered
(
    key UInt64,
    ver UInt64,
    v UInt8,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_scattered;

-- Four parts with interleaved keys, so each part covers the whole key range.
INSERT INTO t_scattered SELECT number * 4 + 0, 1, intHash64(number * 4 + 0) % 127 = 0 FROM numbers(1000);
INSERT INTO t_scattered SELECT number * 4 + 1, 1, intHash64(number * 4 + 1) % 127 = 0 FROM numbers(1000);
INSERT INTO t_scattered SELECT number * 4 + 2, 1, intHash64(number * 4 + 2) % 127 = 0 FROM numbers(1000);
INSERT INTO t_scattered SELECT number * 4 + 3, 1, intHash64(number * 4 + 3) % 127 = 0 FROM numbers(1000);
-- Newer versions: hide some old matches and add some new ones.
INSERT INTO t_scattered SELECT number, 2, intHash64(number) % 251 = 0 FROM numbers(4000)
    WHERE intHash64(number) % 127 = 0 AND number % 2 = 0 OR intHash64(number) % 251 = 0;

SELECT 'single column key';

SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1;
SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1 SETTINGS use_skip_indexes = 0;

-- The number of granules that the recovery pass must select, computed from the primary key index:
-- the granules whose interval of keys intersects the interval of a granule selected by the skip index.
WITH
    granules AS
    (
        SELECT i1.part_name AS part_name, i1.mark_number AS g, i1.key AS lo, i2.key AS hi
        FROM mergeTreeIndex(currentDatabase(), t_scattered) AS i1
        INNER JOIN mergeTreeIndex(currentDatabase(), t_scattered) AS i2
            ON i1.part_name = i2.part_name AND i2.mark_number = i1.mark_number + 1
    ),
    (
        SELECT groupArray((lo, hi)) FROM granules
        WHERE (part_name, g) IN (SELECT _part, _part_granule_offset FROM t_scattered WHERE v = 1 SETTINGS use_skip_indexes = 0)
    ) AS selected
SELECT countIf(arrayExists(s -> lo <= s.2 AND hi >= s.1, selected)) FROM granules;

-- The number of granules that the recovery pass actually selects.
SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT key FROM t_scattered FINAL WHERE v = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

SELECT 'single column key with primary key condition';

SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1 AND key < 2000;
SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1 AND key < 2000 SETTINGS use_skip_indexes = 0;

-- Only the granules selected by the primary key are candidates.
WITH
    granules AS
    (
        SELECT i1.part_name AS part_name, i1.mark_number AS g, i1.key AS lo, i2.key AS hi
        FROM mergeTreeIndex(currentDatabase(), t_scattered) AS i1
        INNER JOIN mergeTreeIndex(currentDatabase(), t_scattered) AS i2
            ON i1.part_name = i2.part_name AND i2.mark_number = i1.mark_number + 1
        WHERE i1.key < 2000
    ),
    (
        SELECT groupArray((lo, hi)) FROM granules
        WHERE (part_name, g) IN (SELECT _part, _part_granule_offset FROM t_scattered WHERE v = 1 SETTINGS use_skip_indexes = 0)
    ) AS selected
SELECT countIf(arrayExists(s -> lo <= s.2 AND hi >= s.1, selected)) FROM granules;

SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT key FROM t_scattered FINAL WHERE v = 1 AND key < 2000
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

DROP TABLE t_scattered;

-- A composite key with long runs of equal values, so granule borders often have equal keys.
DROP TABLE IF EXISTS t_scattered_composite;

CREATE TABLE t_scattered_composite
(
    a UInt64,
    b UInt64,
    ver UInt64,
    v UInt8,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY (a, b)
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_scattered_composite;

INSERT INTO t_scattered_composite SELECT intDiv(number, 50), number % 5, 1, intHash64(number) % 97 = 0 FROM numbers(2500);
INSERT INTO t_scattered_composite SELECT intDiv(number, 30), number % 3, 1, intHash64(number + 1) % 97 = 0 FROM numbers(2500);
INSERT INTO t_scattered_composite SELECT intDiv(number, 70), number % 7, 1, intHash64(number + 2) % 97 = 0 FROM numbers(2500);
INSERT INTO t_scattered_composite SELECT intDiv(number, 40), number % 2, 2, intHash64(number + 3) % 199 = 0 FROM numbers(2500);

SELECT 'composite key';

SELECT count(), sum(a), sum(b) FROM t_scattered_composite FINAL WHERE v = 1;
SELECT count(), sum(a), sum(b) FROM t_scattered_composite FINAL WHERE v = 1 SETTINGS use_skip_indexes = 0;

WITH
    granules AS
    (
        SELECT i1.part_name AS part_name, i1.mark_number AS g, (i1.a, i1.b) AS lo, (i2.a, i2.b) AS hi
        FROM mergeTreeIndex(currentDatabase(), t_scattered_composite) AS i1
        INNER JOIN mergeTreeIndex(currentDatabase(), t_scattered_composite) AS i2
            ON i1.part_name = i2.part_name AND i2.mark_number = i1.mark_number + 1
    ),
    (
        SELECT groupArray((lo, hi)) FROM granules
        WHERE (part_name, g) IN (SELECT _part, _part_granule_offset FROM t_scattered_composite WHERE v = 1 SETTINGS use_skip_indexes = 0)
    ) AS selected
SELECT countIf(arrayExists(s -> lo <= s.2 AND hi >= s.1, selected)) FROM granules;

SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT a, b FROM t_scattered_composite FINAL WHERE v = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

DROP TABLE t_scattered_composite;
