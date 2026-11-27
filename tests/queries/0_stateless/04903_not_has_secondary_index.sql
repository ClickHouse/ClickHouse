-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: granule counts depend on the index granularity and on the skip index settings.

-- Index analysis folds `NOT has(constant_array, key)` into the single `notHas` leaf, the same way it folds
-- `NOT key IN set` into `notIn`. It deliberately does not fold `NOT has(column, needle)`: `KeyCondition` cannot use
-- that shape anyway, while the text index and the bloom filter index analyzers understand `has` but not `notHas`, so
-- folding it would cost them the atom - and, for a text index, the direct read from the index.
-- In the reverse direction, `NOT notHas(column, needle)` does fold back into `has`, preserving the same index path.
-- These checks pin the secondary index behavior of `NOT has(column, needle)` down outside the primary key path.

SET explain_query_plan_default = 'legacy';
SET query_plan_direct_read_from_text_index = 0;

SELECT '-- bloom_filter index on Array(UInt64)';

DROP TABLE IF EXISTS t_not_has_bloom_filter;
CREATE TABLE t_not_has_bloom_filter (id UInt64, arr Array(UInt64), INDEX idx arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

-- 16 granules of 4 rows; the elements of a granule are either {0, 1, 2, 3} or {4, 5, 6, 7}, alternating.
INSERT INTO t_not_has_bloom_filter SELECT number, [number % 8] FROM numbers(64);

-- { echo }

SELECT count() FROM t_not_has_bloom_filter WHERE has(arr, 3);
SELECT count() FROM t_not_has_bloom_filter WHERE NOT has(arr, 3);
SELECT count() FROM t_not_has_bloom_filter WHERE NOT has(arr, 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3);
SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_bloom_filter WHERE NOT has(arr, 3) AND has(arr, 5);
SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3) AND has(arr, 5);
SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3) AND has(arr, 5) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3) OR has(arr, 5);
SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3) OR has(arr, 5) SETTINGS use_skip_indexes = 0;

-- The positive atom prunes half of the granules.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_bloom_filter WHERE has(arr, 3)) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
-- `NOT has` keeps the atom, so the index is still analyzed. It prunes nothing: a bloom filter only answers "the needle
-- may be present in this granule", so the negation of that answer is always "may be true".
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_bloom_filter WHERE NOT has(arr, 3)) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
-- A positive sibling in the same conjunction prunes next to the negated atom.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_bloom_filter WHERE NOT has(arr, 3) AND has(arr, 5)) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_bloom_filter WHERE notHas(arr, 3) AND has(arr, 5)) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';

DROP TABLE t_not_has_bloom_filter;

SELECT '-- tokenbf_v1 index on Array(String)';

DROP TABLE IF EXISTS t_not_has_tokenbf;
CREATE TABLE t_not_has_tokenbf (id UInt64, arr Array(String), INDEX idx arr TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_not_has_tokenbf SELECT number, [concat('w', toString(number % 8))] FROM numbers(64);

SELECT count() FROM t_not_has_tokenbf WHERE has(arr, 'w3');
SELECT count() FROM t_not_has_tokenbf WHERE NOT has(arr, 'w3');
SELECT count() FROM t_not_has_tokenbf WHERE NOT has(arr, 'w3') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_tokenbf WHERE notHas(arr, 'w3');
SELECT count() FROM t_not_has_tokenbf WHERE notHas(arr, 'w3') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_tokenbf WHERE NOT has(arr, 'w3') AND has(arr, 'w5');
SELECT count() FROM t_not_has_tokenbf WHERE notHas(arr, 'w3') AND has(arr, 'w5');
SELECT count() FROM t_not_has_tokenbf WHERE notHas(arr, 'w3') AND has(arr, 'w5') SETTINGS use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_tokenbf WHERE has(arr, 'w3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_tokenbf WHERE NOT has(arr, 'w3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_tokenbf WHERE NOT has(arr, 'w3') AND has(arr, 'w5')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';

DROP TABLE t_not_has_tokenbf;

SELECT '-- text index with the array tokenizer on Array(String)';

DROP TABLE IF EXISTS t_not_has_text;
CREATE TABLE t_not_has_text (id UInt64, arr Array(String), INDEX idx arr TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_not_has_text SELECT number, [concat('w', toString(number % 8))] FROM numbers(64);

SELECT count() FROM t_not_has_text WHERE has(arr, 'w3');
SELECT count() FROM t_not_has_text WHERE NOT has(arr, 'w3');
SELECT count() FROM t_not_has_text WHERE NOT has(arr, 'w3') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_text WHERE notHas(arr, 'w3');
SELECT count() FROM t_not_has_text WHERE notHas(arr, 'w3') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_not_has_text WHERE NOT has(arr, 'w3') AND has(arr, 'w5');
SELECT count() FROM t_not_has_text WHERE notHas(arr, 'w3') AND has(arr, 'w5');
SELECT count() FROM t_not_has_text WHERE notHas(arr, 'w3') AND has(arr, 'w5') SETTINGS use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_text WHERE has(arr, 'w3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_text WHERE NOT has(arr, 'w3')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_not_has_text WHERE NOT has(arr, 'w3') AND has(arr, 'w5')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';

-- The direct read from the text index reads `has` through a virtual column, and `NOT has` negates that column.
-- Folding `NOT has(column, needle)` into `notHas` would have taken this away.
SET query_plan_direct_read_from_text_index = 1;

SELECT max(id) FROM t_not_has_text WHERE has(arr, 'w3');
SELECT max(id) FROM t_not_has_text WHERE NOT has(arr, 'w3');
SELECT max(id) FROM t_not_has_text WHERE notHas(arr, 'w3');
SELECT max(id) FROM t_not_has_text WHERE notHas(arr, 'w3') SETTINGS use_skip_indexes = 0;
SELECT max(id) FROM t_not_has_text WHERE NOT has(arr, 'w3') AND has(arr, 'w5');
SELECT max(id) FROM t_not_has_text WHERE notHas(arr, 'w3') AND has(arr, 'w5');

-- `max(id)` rather than `count()` because a bare `count()` over `has` is answered by the text index alone
-- (`ReadFromTextIndexCount`) and never materializes the virtual column. Both `has` and `NOT has` read it;
-- the `notHas` leaf is only understood by `KeyCondition`, so a directly written `notHas` does not.
SELECT countIf(position(explain, '__text_index_idx_has_') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT max(id) FROM t_not_has_text WHERE has(arr, 'w3'));
SELECT countIf(position(explain, '__text_index_idx_has_') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT max(id) FROM t_not_has_text WHERE NOT has(arr, 'w3'));
SELECT countIf(position(explain, '__text_index_idx_has_') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT max(id) FROM t_not_has_text WHERE notHas(arr, 'w3'));
SELECT countIf(position(explain, '__text_index_idx_has_') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT max(id) FROM t_not_has_text WHERE NOT notHas(arr, 'w3'));

DROP TABLE t_not_has_text;
