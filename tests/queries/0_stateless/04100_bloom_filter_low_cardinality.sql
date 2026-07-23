-- Bloom filter index over LowCardinality columns must return the same rows as the
-- equivalent plain String index (issue #106372). The hashes are a subset of the
-- per-row hashes, so the bloom filter must not drop any matching row.
--
-- Every query sets force_data_skipping_indices so the test fails if the bloom index
-- is not actually applied (otherwise a full scan would mask an index that drops rows).

-- Scalar LowCardinality(String)
DROP TABLE IF EXISTS bf_lc_scalar;
CREATE TABLE bf_lc_scalar
(
    id UInt64,
    s LowCardinality(String),
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO bf_lc_scalar SELECT number, concat('v', toString(number % 5)) FROM numbers(100);

SELECT 'scalar eq';
SELECT count() FROM bf_lc_scalar WHERE s = 'v3' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'scalar in';
SELECT count() FROM bf_lc_scalar WHERE s IN ('v1', 'v4') SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'scalar absent';
SELECT count() FROM bf_lc_scalar WHERE s = 'nope' SETTINGS force_data_skipping_indices = 'idx_s';

-- Array(LowCardinality(String))  -- mirrors disttrace label_names
DROP TABLE IF EXISTS bf_lc_array;
CREATE TABLE bf_lc_array
(
    id UInt64,
    arr Array(LowCardinality(String)),
    INDEX idx_arr arr TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO bf_lc_array
SELECT number, arrayMap(x -> concat('t', toString(x % 7)), range(1 + (number % 4)))
FROM numbers(100);

SELECT 'array has';
SELECT count() FROM bf_lc_array WHERE has(arr, 't3') SETTINGS force_data_skipping_indices = 'idx_arr';
SELECT 'array hasAny';
SELECT count() FROM bf_lc_array WHERE hasAny(arr, ['t1', 't6']) SETTINGS force_data_skipping_indices = 'idx_arr';
SELECT 'array absent';
SELECT count() FROM bf_lc_array WHERE has(arr, 'nope') SETTINGS force_data_skipping_indices = 'idx_arr';

-- LowCardinality(Nullable(String))
DROP TABLE IF EXISTS bf_lc_nullable;
CREATE TABLE bf_lc_nullable
(
    id UInt64,
    s LowCardinality(Nullable(String)),
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO bf_lc_nullable
SELECT number, if(number % 3 = 0, NULL, concat('n', toString(number % 4))) FROM numbers(100);

SELECT 'nullable eq';
SELECT count() FROM bf_lc_nullable WHERE s = 'n2' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'nullable absent';
SELECT count() FROM bf_lc_nullable WHERE s = 'nope' SETTINGS force_data_skipping_indices = 'idx_s';

-- Large dictionary spread across many skip-index granules. The block dictionary
-- is much larger than any one granule's distinct set, so this exercises the path
-- that hashes only each granule's distinct entries (and must not drop a needed
-- hash or admit an absent one).
DROP TABLE IF EXISTS bf_lc_big_dict;
CREATE TABLE bf_lc_big_dict
(
    id UInt64,
    s LowCardinality(String),
    arr Array(LowCardinality(String)),
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
    INDEX idx_arr arr TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO bf_lc_big_dict
SELECT number,
       concat('k', toString(number)),
       arrayMap(x -> concat('e', toString(number + x)), range(1 + (number % 5)))
FROM numbers(1000);

SELECT 'big scalar present';
SELECT count() FROM bf_lc_big_dict WHERE s = 'k500' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'big scalar present first';
SELECT count() FROM bf_lc_big_dict WHERE s = 'k0' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'big scalar absent';
SELECT count() FROM bf_lc_big_dict WHERE s = 'nope' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'big array present';
SELECT count() FROM bf_lc_big_dict WHERE has(arr, 'e500') SETTINGS force_data_skipping_indices = 'idx_arr';
SELECT 'big array absent';
SELECT count() FROM bf_lc_big_dict WHERE has(arr, 'nope') SETTINGS force_data_skipping_indices = 'idx_arr';

DROP TABLE bf_lc_scalar;
DROP TABLE bf_lc_array;
DROP TABLE bf_lc_nullable;
DROP TABLE bf_lc_big_dict;
