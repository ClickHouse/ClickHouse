-- Tags: no-fasttest
-- Test: Verify that ngrambf_v1/tokenbf_v1 indexes on mapKeys don't crash/assert
-- when queries use non-map functions (e.g., equals) on the Map column directly.
-- Covers: MergeTreeIndexBloomFilterText.cpp:591-592 (return false for non-map functions when map_key_index is set)


DROP TABLE IF EXISTS t_bf_map_nonmap_77305;

CREATE TABLE t_bf_map_nonmap_77305 (
    id UInt32,
    c0 Map(FixedString(2), String),
    INDEX i0 mapKeys(c0) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_bf_map_nonmap_77305 VALUES (1, {'K0':'V0'}), (2, {'K1':'V1'});

-- This query exercises the equals comparison on a Map column with a mapKeys ngrambf index.
-- Before the fix, this would hit an assertion in debug builds due to uninitialized key_index.
-- After the fix, the bloom filter index is correctly not used (returns false), and the query
-- proceeds normally (failing with a type error since you can't compare String to Map).
SELECT 1 FROM t_bf_map_nonmap_77305 PREWHERE toNullable('V0') = c0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Also test with tokenbf_v1 to exercise the same code path
DROP TABLE IF EXISTS t_bf_map_nonmap2_77305;

CREATE TABLE t_bf_map_nonmap2_77305 (
    id UInt32,
    c0 Map(String, String),
    INDEX i0 mapKeys(c0) TYPE tokenbf_v1(256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_bf_map_nonmap2_77305 VALUES (1, {'K0':'V0'}), (2, {'K1':'V1'});

SELECT 1 FROM t_bf_map_nonmap2_77305 PREWHERE toNullable('V0') = c0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Verify normal map operations still work correctly with the index
SELECT id FROM t_bf_map_nonmap_77305 WHERE mapContains(c0, 'K0') ORDER BY id;
SELECT id FROM t_bf_map_nonmap2_77305 WHERE mapContains(c0, 'K0') ORDER BY id;

DROP TABLE IF EXISTS t_bf_map_nonmap_77305;
DROP TABLE IF EXISTS t_bf_map_nonmap2_77305;
