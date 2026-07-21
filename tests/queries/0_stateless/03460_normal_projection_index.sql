-- { echo ON }

SET enable_analyzer = 1;
-- enable projection for parallel replicas
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
DROP TABLE IF EXISTS test_simple_projection;

CREATE TABLE test_simple_projection
(
    id UInt64,
    event_date Date,
    user_id UInt32,
    url String,
    region String,
    PROJECTION region_proj
    (
        SELECT _part_offset ORDER BY region
    ),
    PROJECTION user_id_proj
    (
        SELECT _part_offset ORDER BY user_id
    )
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS
    index_granularity = 1, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO test_simple_projection VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe');
INSERT INTO test_simple_projection VALUES (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west');
INSERT INTO test_simple_projection VALUES (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west');
INSERT INTO test_simple_projection VALUES (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west');
INSERT INTO test_simple_projection VALUES (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

OPTIMIZE TABLE test_simple_projection FINAL;

-- aggressively use projection index
SET min_table_rows_to_use_projection_index = 0;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region = 'europe' AND user_id = 101)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_simple_projection WHERE region = 'europe' AND user_id = 101 ORDER BY ALL;

-- region_proj is enough to filter part
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region = 'zzz' AND user_id = 101)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_simple_projection WHERE region = 'zzz' AND user_id = 101 ORDER BY ALL;

-- narrowing filter via user_id_proj
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region = 'us_west' AND user_id = 106)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_simple_projection WHERE region = 'us_west' AND user_id = 106 ORDER BY ALL;

-- it's not possible to use different projection indexes with or filter
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region = 'asia' OR user_id = 101)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_simple_projection WHERE region = 'asia' OR user_id = 101 ORDER BY ALL;

-- Fuzzer
SELECT *, _part_offset = (isNullable(1) = toUInt128(6)), * FROM test_simple_projection PREWHERE (101 = user_id) = ignore(255, isZeroOrNull(assumeNotNull(0))) WHERE (106 = user_id) AND (region = 'us_west');

DROP TABLE test_simple_projection;

-- verify projection index can filter individual matching rows at top, middle, and bottom of a single granule.

DROP TABLE IF EXISTS test_projection_granule_edge_cases;

CREATE TABLE test_projection_granule_edge_cases
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj
    (
        SELECT _part_offset ORDER BY region
    )
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 16, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO test_projection_granule_edge_cases VALUES (0, 'top_region', 100);
INSERT INTO test_projection_granule_edge_cases SELECT number + 1, 'other_region', 101 FROM numbers(6);
INSERT INTO test_projection_granule_edge_cases VALUES (7, 'mid_region', 102);
INSERT INTO test_projection_granule_edge_cases SELECT number + 8, 'other_region', 103 FROM numbers(6);
INSERT INTO test_projection_granule_edge_cases VALUES (15, 'bol_region', 104);

-- add more data to ensure projection index is triggered during query planning
INSERT INTO test_projection_granule_edge_cases SELECT number + 100, 'unknown_region', 999 FROM numbers(1000);

OPTIMIZE TABLE test_projection_granule_edge_cases FINAL;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_projection_granule_edge_cases WHERE region = 'top_region')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_projection_granule_edge_cases WHERE region = 'top_region' ORDER BY ALL;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_projection_granule_edge_cases WHERE region = 'mid_region')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_projection_granule_edge_cases WHERE region = 'mid_region' ORDER BY ALL;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_projection_granule_edge_cases WHERE region = 'bol_region')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_projection_granule_edge_cases WHERE region = 'bol_region' ORDER BY ALL;

DROP TABLE test_projection_granule_edge_cases;

-- check partially materialized projection index, it should only affect related parts

DROP TABLE IF EXISTS test_partial_projection;

CREATE TABLE test_partial_projection
(
    id UInt64,
    region String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO test_partial_projection VALUES (1, 'us'), (2, 'eu'), (3, 'cn');

ALTER TABLE test_partial_projection ADD PROJECTION region_proj (SELECT _part_offset ORDER BY region);

INSERT INTO test_partial_projection VALUES (4, 'cn'), (5, 'ru'), (6, 'br');

SET min_table_rows_to_use_projection_index = 0;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_partial_projection WHERE region = 'ru')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_partial_projection WHERE region = 'ru' ORDER BY ALL;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_partial_projection WHERE region = 'cn')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');
SELECT * FROM test_partial_projection WHERE region = 'cn' ORDER BY ALL;

DROP TABLE test_partial_projection;
