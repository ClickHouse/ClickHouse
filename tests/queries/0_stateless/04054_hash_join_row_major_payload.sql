-- Tests for `min_columns_for_hash_join_row_store` — storing the right-side
-- payload of a hash join in row-major form for fixed-and-contiguous columns.
--
-- Each scenario runs the same query with the feature disabled (setting = 0)
-- and aggressively enabled (setting = 1), hashes the result, and asserts the
-- two hashes match. Any divergence means the row-major path differs from the
-- columnar baseline.
--
-- Scenarios cover the code paths the refactor touches:
--   * INNER / LEFT / RIGHT / FULL / ASOF
--   * Mixed (fixed + String) and all-fixed right tables
--   * joinGet / joinGetOrNull
--   * Multi-disjunct ON (triggers `save_key_columns`)
--   * parallel_hash (ConcurrentHashJoin nullmap path)
--   * Duplicated right keys (RowRefList path)
--   * LIMIT + max_joined_block_bytes (`buildOutputFromBlocksLimitAndOffset`)

DROP TABLE IF EXISTS rs_l;
DROP TABLE IF EXISTS rs_r_mixed;
DROP TABLE IF EXISTS rs_r_all_fixed;
DROP TABLE IF EXISTS rs_r_asof;
DROP TABLE IF EXISTS rs_r_nullable_keys;
DROP TABLE IF EXISTS rs_storage_join;

CREATE TABLE rs_l (k Int64, k2 Int64, lv String) ENGINE = MergeTree ORDER BY k;

-- Right table with mixed fixed (Int32, Int64, UInt8, Decimal64) + variable (String) columns
CREATE TABLE rs_r_mixed
(
    k Int64,
    a Int32,
    b Int64,
    c UInt8,
    d Decimal64(2),
    s String
) ENGINE = MergeTree ORDER BY k;

-- Right table with only fixed columns (every payload column goes to the row store)
CREATE TABLE rs_r_all_fixed
(
    k Int64,
    a Int32,
    b UInt64,
    c UInt8,
    f Float64
) ENGINE = MergeTree ORDER BY k;

CREATE TABLE rs_r_asof
(
    k Int32,
    t DateTime,
    v1 Int32,
    v2 Int64,
    v3 UInt8
) ENGINE = MergeTree ORDER BY (k, t);

-- Explicit NULL keys exercise the ConcurrentHashJoin nullmap rebuild path
CREATE TABLE rs_r_nullable_keys
(
    k Nullable(Int64),
    a Int32,
    b Int64,
    c UInt8
) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;

INSERT INTO rs_l SELECT number % 500, number % 250, concat('L', toString(number)) FROM numbers(2000);

INSERT INTO rs_r_mixed
SELECT number % 500, toInt32(number), toInt64(number * 2), toUInt8(number % 255),
       toDecimal64(number + 0.5, 2), concat('R', toString(number))
FROM numbers(2000);

INSERT INTO rs_r_all_fixed
SELECT number % 500, toInt32(number), toUInt64(number * 3), toUInt8(number % 200), toFloat64(number) + 0.25
FROM numbers(2000);

INSERT INTO rs_r_asof
SELECT number % 100, toDateTime('2024-01-01 00:00:00') + number,
       toInt32(number), toInt64(number * 7), toUInt8(number % 128)
FROM numbers(1000);

INSERT INTO rs_r_nullable_keys
SELECT if(number % 23 = 0, NULL, toInt64(number % 500)), toInt32(number), toInt64(number * 2), toUInt8(number % 255)
FROM numbers(2000);

SELECT '--- INNER JOIN, mixed right columns ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c, r.d, r.s))
    FROM rs_l l INNER JOIN rs_r_mixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c, r.d, r.s))
    FROM rs_l l INNER JOIN rs_r_mixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS inner_mixed_match;

SELECT '--- INNER JOIN, all-fixed right columns (every column goes to row store) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c, r.f))
    FROM rs_l l INNER JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c, r.f))
    FROM rs_l l INNER JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS inner_all_fixed_match;

SELECT '--- LEFT JOIN with unmatched rows ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c))
    FROM rs_l l LEFT JOIN (SELECT * FROM rs_r_mixed WHERE a < 1000) r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c))
    FROM rs_l l LEFT JOIN (SELECT * FROM rs_r_mixed WHERE a < 1000) r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS left_unmatched_match;

SELECT '--- RIGHT JOIN (save_key_columns triggers prepended keys in saved_block_sample) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.lv, r.k, r.a, r.b, r.c))
    FROM rs_l l RIGHT JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, l.lv, r.k, r.a, r.b, r.c))
    FROM rs_l l RIGHT JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS right_all_fixed_match;

SELECT '--- FULL JOIN, non-joined right rows go through NonJoinedBlockInputStream ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.lv, r.k, r.a, r.b, r.c))
    FROM rs_l l FULL JOIN (SELECT * FROM rs_r_all_fixed WHERE k < 400) r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, l.lv, r.k, r.a, r.b, r.c))
    FROM rs_l l FULL JOIN (SELECT * FROM rs_r_all_fixed WHERE k < 400) r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS full_match;

SELECT '--- ASOF JOIN (ASOF key prepended to saved_block_sample) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, r.v1, r.v2, r.v3))
    FROM (SELECT toInt32(number % 100) AS k, toDateTime('2024-01-01 00:00:00') + number * 2 AS t FROM numbers(500)) l
    ASOF JOIN rs_r_asof r ON l.k = r.k AND l.t >= r.t
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, r.v1, r.v2, r.v3))
    FROM (SELECT toInt32(number % 100) AS k, toDateTime('2024-01-01 00:00:00') + number * 2 AS t FROM numbers(500)) l
    ASOF JOIN rs_r_asof r ON l.k = r.k AND l.t >= r.t
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS asof_match;

SELECT '--- Multi-disjunct ON (forces save_key_columns via multiple_disjuncts) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.k2, l.lv, r.k, r.a, r.b, r.c))
    FROM rs_l l INNER JOIN rs_r_all_fixed r ON l.k = r.k OR l.k2 = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0, join_use_nulls = 0
) = (
    SELECT sum(cityHash64(l.k, l.k2, l.lv, r.k, r.a, r.b, r.c))
    FROM rs_l l INNER JOIN rs_r_all_fixed r ON l.k = r.k OR l.k2 = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1, join_use_nulls = 0
) AS multi_disjunct_match;

SELECT '--- parallel_hash with NULL right keys (ConcurrentHashJoin nullmap path) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c))
    FROM rs_l l FULL JOIN rs_r_nullable_keys r ON l.k = r.k
    SETTINGS join_algorithm = 'parallel_hash', min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, l.lv, r.a, r.b, r.c))
    FROM rs_l l FULL JOIN rs_r_nullable_keys r ON l.k = r.k
    SETTINGS join_algorithm = 'parallel_hash', min_columns_for_hash_join_row_store = 1
) AS parallel_hash_nullmap_match;

SELECT '--- Row-list path (many duplicate right keys, ALL strictness) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, r.a, r.b, r.c, r.f))
    FROM (SELECT toInt64(number % 50) AS k FROM numbers(200)) l
    INNER JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, r.a, r.b, r.c, r.f))
    FROM (SELECT toInt64(number % 50) AS k FROM numbers(200)) l
    INNER JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS row_list_match;

SELECT '--- max_joined_block_bytes split (buildOutputFromBlocksLimitAndOffset) ---';
SELECT
(
    SELECT sum(cityHash64(l.k, r.a, r.b, r.c, r.f))
    FROM (SELECT toInt64(number % 50) AS k FROM numbers(200)) l
    INNER JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0,
             max_joined_block_rows = 64, max_joined_block_bytes = 4096, joined_block_split_single_row = 1
) = (
    SELECT sum(cityHash64(l.k, r.a, r.b, r.c, r.f))
    FROM (SELECT toInt64(number % 50) AS k FROM numbers(200)) l
    INNER JOIN rs_r_all_fixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 1,
             max_joined_block_rows = 64, max_joined_block_bytes = 4096, joined_block_split_single_row = 1
) AS limited_block_match;

SELECT '--- joinGet on Join engine storage ---';
CREATE TABLE rs_storage_join (k Int64, a Int32, b Int64, c UInt8, f Float64)
    ENGINE = Join(ANY, LEFT, k);
INSERT INTO rs_storage_join SELECT k, a, b, c, f FROM rs_r_all_fixed;

SELECT
(
    SELECT sum(cityHash64(k, joinGet('rs_storage_join', 'a', k),
                             joinGet('rs_storage_join', 'b', k),
                             joinGet('rs_storage_join', 'c', k),
                             joinGet('rs_storage_join', 'f', k)))
    FROM rs_l
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(k, joinGet('rs_storage_join', 'a', k),
                             joinGet('rs_storage_join', 'b', k),
                             joinGet('rs_storage_join', 'c', k),
                             joinGet('rs_storage_join', 'f', k)))
    FROM rs_l
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS join_get_match;

SELECT '--- joinGetOrNull (result columns nullable, source columns not) ---';
SELECT
(
    SELECT sum(cityHash64(k,
        ifNull(joinGetOrNull('rs_storage_join', 'a', k), toInt32(-1)),
        ifNull(joinGetOrNull('rs_storage_join', 'b', k), toInt64(-1)),
        ifNull(joinGetOrNull('rs_storage_join', 'c', k), toUInt8(0))))
    FROM rs_l
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(k,
        ifNull(joinGetOrNull('rs_storage_join', 'a', k), toInt32(-1)),
        ifNull(joinGetOrNull('rs_storage_join', 'b', k), toInt64(-1)),
        ifNull(joinGetOrNull('rs_storage_join', 'c', k), toUInt8(0))))
    FROM rs_l
    SETTINGS min_columns_for_hash_join_row_store = 1
) AS join_get_or_null_match;

SELECT '--- Threshold honored: setting=10 leaves the feature inactive here ---';
SELECT
(
    SELECT sum(cityHash64(l.k, r.a, r.b, r.c))
    FROM rs_l l INNER JOIN rs_r_mixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 0
) = (
    SELECT sum(cityHash64(l.k, r.a, r.b, r.c))
    FROM rs_l l INNER JOIN rs_r_mixed r ON l.k = r.k
    SETTINGS min_columns_for_hash_join_row_store = 10
) AS threshold_above_fixed_count_match;

DROP TABLE rs_storage_join;
DROP TABLE rs_r_nullable_keys;
DROP TABLE rs_r_asof;
DROP TABLE rs_r_all_fixed;
DROP TABLE rs_r_mixed;
DROP TABLE rs_l;
