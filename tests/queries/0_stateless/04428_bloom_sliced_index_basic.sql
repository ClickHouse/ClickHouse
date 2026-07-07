-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_index_basic;

SELECT '-- experimental gate';
CREATE TABLE bloom_sliced_index_gate
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_bloom_sliced_index = 1;

SELECT '-- create and validate';

CREATE TABLE bloom_sliced_index_basic
(
    id UInt64,
    text String,
    nullable_text Nullable(String),
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;

INSERT INTO bloom_sliced_index_basic
SELECT
    number,
    multiIf(number = 42, 'rare alpha common', number % 2 = 0, 'common even', 'common odd'),
    if(number = 7, NULL, multiIf(number = 42, 'rare alpha common', number % 2 = 0, 'common even', 'common odd'))
FROM numbers(100);

SELECT type, granularity, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'bloom_sliced_index_basic' AND name = 'idx';

SELECT '-- correctness';
SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_index_basic WHERE hasAllTokens(text, 'rare alpha') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') OR hasToken(text, 'odd') SETTINGS force_data_skipping_indices = 'idx', use_skip_indexes_for_disjunctions = 1;

DROP TABLE IF EXISTS bloom_sliced_index_grouped;
CREATE TABLE bloom_sliced_index_grouped
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(bits = 2048, hashes = 4, rows_per_signature = 4) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;
INSERT INTO bloom_sliced_index_grouped VALUES (1, 'rare alpha'), (2, 'common'), (3, 'common'), (4, 'common');
SELECT count() FROM bloom_sliced_index_grouped WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE bloom_sliced_index_grouped;

SELECT '-- explain pruning';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Description: bloom_sliced%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Name: idx%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 1/%';

SELECT '-- dense no-prune bypass';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Name: idx%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 100/100%';
SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx';


SELECT '-- unsupported predicates fail open';
SELECT count() FROM bloom_sliced_index_basic WHERE startsWith(text, 'rare') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_index_basic WHERE startsWith(text, 'rare') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_basic WHERE startsWith(text, 'rare') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%Name: idx%';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') AND startsWith(text, 'rare') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';
SELECT count() FROM bloom_sliced_index_basic WHERE hasToken(text, 'alpha') AND startsWith(text, 'rare') SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_bloom_sliced_index = 1;

SELECT '-- attach/detach serialization';
DROP TABLE IF EXISTS bloom_sliced_index_roundtrip;
CREATE TABLE bloom_sliced_index_roundtrip
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 1024, hashes = 3, min_hashes = 3, rows_per_signature = 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;
INSERT INTO bloom_sliced_index_roundtrip SELECT number, if(number = 11, 'rare alpha', 'common') FROM numbers(20);
SELECT count() FROM bloom_sliced_index_roundtrip WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx';
DETACH TABLE bloom_sliced_index_roundtrip;
ATTACH TABLE bloom_sliced_index_roundtrip;
SELECT count() FROM bloom_sliced_index_roundtrip WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_roundtrip WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 2/20%';
DROP TABLE bloom_sliced_index_roundtrip;

SELECT '-- nullable string support';
DROP TABLE IF EXISTS bloom_sliced_index_nullable;
CREATE TABLE bloom_sliced_index_nullable
(
    id UInt64,
    text Nullable(String),
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 512, hashes = 3, min_hashes = 3, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;
INSERT INTO bloom_sliced_index_nullable
SELECT number, if(number IN (5, 6), NULL, multiIf(number = 7, 'rare beta', number % 2 = 0, 'common even', 'common odd'))
FROM numbers(10);
SELECT count() FROM bloom_sliced_index_nullable WHERE hasToken(text, 'beta') SETTINGS force_data_skipping_indices = 'idx';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_index_nullable WHERE hasToken(text, 'beta') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 1/10%';
DROP TABLE bloom_sliced_index_nullable;

DROP TABLE bloom_sliced_index_basic;

SELECT '-- negative: type and arguments';

CREATE TABLE bloom_sliced_index_bad_type
(
    id UInt64,
    x UInt64,
    INDEX idx x TYPE bloom_sliced GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

CREATE TABLE bloom_sliced_index_bad_arg
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(bits = 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE bloom_sliced_index_low_cardinality_bad_nested
(
    id UInt64,
    text LowCardinality(UInt64),
    INDEX idx text TYPE bloom_sliced GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }
