-- `use_skip_indexes_if_final_exact_mode` must add back exactly the granules whose primary key
-- interval intersects the primary key interval of some granule selected by the skip index.
-- The selected granules are scattered over parts with overlapping key ranges.

SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;
SET query_plan_optimize_lazy_final = 0;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;

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
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_scattered;

-- Four parts with interleaved keys, so each part covers the whole key range.
INSERT INTO t_scattered SELECT number * 4 + 0, 1, intHash64(number * 4 + 0) % 509 = 0 FROM numbers(4000);
INSERT INTO t_scattered SELECT number * 4 + 1, 1, intHash64(number * 4 + 1) % 509 = 0 FROM numbers(4000);
INSERT INTO t_scattered SELECT number * 4 + 2, 1, intHash64(number * 4 + 2) % 509 = 0 FROM numbers(4000);
INSERT INTO t_scattered SELECT number * 4 + 3, 1, intHash64(number * 4 + 3) % 509 = 0 FROM numbers(4000);
-- Newer versions: hide some old matches and add some new ones.
INSERT INTO t_scattered SELECT number, 2, intHash64(number) % 1013 = 0 FROM numbers(16000) WHERE intHash64(number) % 509 = 0 AND number % 2 = 0 OR intHash64(number) % 1013 = 0;

SELECT 'single column key';

SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1;
SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM
(
    SELECT key FROM t_scattered FINAL WHERE v = 1 SETTINGS use_skip_indexes = 0
    EXCEPT
    SELECT key FROM t_scattered FINAL WHERE v = 1
);

-- The number of granules that the recovery pass must select, computed from the primary key index.
SELECT count() FROM
(
    WITH
        granules AS
        (
            SELECT i1.part_name AS part_name, i1.mark_number AS g, i1.key AS lo, i2.key AS hi
            FROM mergeTreeIndex(currentDatabase(), t_scattered) AS i1
            INNER JOIN mergeTreeIndex(currentDatabase(), t_scattered) AS i2
                ON i1.part_name = i2.part_name AND i2.mark_number = i1.mark_number + 1
        ),
        selected AS
        (
            SELECT granules.lo AS lo, granules.hi AS hi
            FROM granules
            INNER JOIN (SELECT DISTINCT _part AS part_name, _part_granule_offset AS g FROM t_scattered WHERE v = 1 SETTINGS use_skip_indexes = 0) AS s
                ON granules.part_name = s.part_name AND granules.g = s.g
        )
    SELECT DISTINCT granules.part_name, granules.g
    FROM granules CROSS JOIN selected
    WHERE granules.lo <= selected.hi AND granules.hi >= selected.lo
);

-- The number of granules that the recovery pass actually selects.
SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT key FROM t_scattered FINAL WHERE v = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

SELECT 'single column key with primary key condition';

SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1 AND key < 8000;
SELECT count(), sum(key) FROM t_scattered FINAL WHERE v = 1 AND key < 8000 SETTINGS use_skip_indexes = 0;

-- Only the granules selected by the primary key are candidates.
SELECT count() FROM
(
    WITH
        granules AS
        (
            SELECT i1.part_name AS part_name, i1.mark_number AS g, i1.key AS lo, i2.key AS hi
            FROM mergeTreeIndex(currentDatabase(), t_scattered) AS i1
            INNER JOIN mergeTreeIndex(currentDatabase(), t_scattered) AS i2
                ON i1.part_name = i2.part_name AND i2.mark_number = i1.mark_number + 1
            WHERE i1.key < 8000
        ),
        selected AS
        (
            SELECT granules.lo AS lo, granules.hi AS hi
            FROM granules
            INNER JOIN (SELECT DISTINCT _part AS part_name, _part_granule_offset AS g FROM t_scattered WHERE v = 1 SETTINGS use_skip_indexes = 0) AS s
                ON granules.part_name = s.part_name AND granules.g = s.g
        )
    SELECT DISTINCT granules.part_name, granules.g
    FROM granules CROSS JOIN selected
    WHERE granules.lo <= selected.hi AND granules.hi >= selected.lo
);

SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT key FROM t_scattered FINAL WHERE v = 1 AND key < 8000
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
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_scattered_composite;

INSERT INTO t_scattered_composite SELECT intDiv(number, 50), number % 5, 1, intHash64(number) % 211 = 0 FROM numbers(10000);
INSERT INTO t_scattered_composite SELECT intDiv(number, 30), number % 3, 1, intHash64(number + 1) % 211 = 0 FROM numbers(10000);
INSERT INTO t_scattered_composite SELECT intDiv(number, 70), number % 7, 1, intHash64(number + 2) % 211 = 0 FROM numbers(10000);
INSERT INTO t_scattered_composite SELECT intDiv(number, 40), number % 2, 2, intHash64(number + 3) % 401 = 0 FROM numbers(10000);

SELECT 'composite key';

SELECT count(), sum(a), sum(b) FROM t_scattered_composite FINAL WHERE v = 1;
SELECT count(), sum(a), sum(b) FROM t_scattered_composite FINAL WHERE v = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM
(
    SELECT a, b FROM t_scattered_composite FINAL WHERE v = 1 SETTINGS use_skip_indexes = 0
    EXCEPT
    SELECT a, b FROM t_scattered_composite FINAL WHERE v = 1
);

SELECT count() FROM
(
    WITH
        granules AS
        (
            SELECT i1.part_name AS part_name, i1.mark_number AS g, (i1.a, i1.b) AS lo, (i2.a, i2.b) AS hi
            FROM mergeTreeIndex(currentDatabase(), t_scattered_composite) AS i1
            INNER JOIN mergeTreeIndex(currentDatabase(), t_scattered_composite) AS i2
                ON i1.part_name = i2.part_name AND i2.mark_number = i1.mark_number + 1
        ),
        selected AS
        (
            SELECT granules.lo AS lo, granules.hi AS hi
            FROM granules
            INNER JOIN (SELECT DISTINCT _part AS part_name, _part_granule_offset AS g FROM t_scattered_composite WHERE v = 1 SETTINGS use_skip_indexes = 0) AS s
                ON granules.part_name = s.part_name AND granules.g = s.g
        )
    SELECT DISTINCT granules.part_name, granules.g
    FROM granules CROSS JOIN selected
    WHERE granules.lo <= selected.hi AND granules.hi >= selected.lo
);

SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT a, b FROM t_scattered_composite FINAL WHERE v = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

DROP TABLE t_scattered_composite;
