-- Bloom filter index over LowCardinality columns must return the same rows as
-- the equivalent plain String index. This covers the dictionary-based hashing
-- path in BloomFilterHash::hashWithColumn (issue #106372). The values hashed
-- are identical to the full-column path, so the bloom filter must not drop any
-- matching row.

SET allow_suspicious_low_cardinality_types = 1;

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
SELECT count() FROM bf_lc_scalar WHERE s = 'v3';
SELECT 'scalar in';
SELECT count() FROM bf_lc_scalar WHERE s IN ('v1', 'v4');
SELECT 'scalar absent';
SELECT count() FROM bf_lc_scalar WHERE s = 'nope';

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
SELECT count() FROM bf_lc_array WHERE has(arr, 't3');
SELECT 'array hasAny';
SELECT count() FROM bf_lc_array WHERE hasAny(arr, ['t1', 't6']);
SELECT 'array absent';
SELECT count() FROM bf_lc_array WHERE has(arr, 'nope');

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
SELECT count() FROM bf_lc_nullable WHERE s = 'n2';
SELECT 'nullable absent';
SELECT count() FROM bf_lc_nullable WHERE s = 'nope';

DROP TABLE bf_lc_scalar;
DROP TABLE bf_lc_array;
DROP TABLE bf_lc_nullable;
