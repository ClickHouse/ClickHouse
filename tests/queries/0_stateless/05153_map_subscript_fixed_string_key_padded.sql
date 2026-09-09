-- A FixedString(N) map key is stored zero-padded to N bytes, so a subscript of at most N bytes names
-- that same key once padded, the way equality compares the two. Each value row prints the subscript
-- answer beside an inline equality-based oracle over mapKeys/mapValues.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_fixed_key;
DROP TABLE IF EXISTS t_lc_fixed_key;
DROP TABLE IF EXISTS t_string_key;
DROP TABLE IF EXISTS t_bloom_key;
DROP TABLE IF EXISTS t_text_key;

CREATE TABLE t_fixed_key (m Map(FixedString(4), UInt8), k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fixed_key SELECT map(CAST('a', 'FixedString(4)'), 7), unhex('6100');

SELECT 'S1 key column, zero-tailed subscript', m[k],
       arraySum(arrayMap((kk, v) -> if(kk = k, v, 0), mapKeys(m), mapValues(m))) FROM t_fixed_key;

SELECT 'S2 constant subscript, no subcolumn rewrite', m[unhex('6100')],
       arraySum(arrayMap((kk, v) -> if(kk = unhex('6100'), v, 0), mapKeys(m), mapValues(m)))
FROM t_fixed_key SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'S3 control, exact width', m[unhex('61000000')],
       arraySum(arrayMap((kk, v) -> if(kk = unhex('61000000'), v, 0), mapKeys(m), mapValues(m))) FROM t_fixed_key;

SELECT 'S4 control, non-zero tail', m[unhex('6162')],
       arraySum(arrayMap((kk, v) -> if(kk = unhex('6162'), v, 0), mapKeys(m), mapValues(m))) FROM t_fixed_key;

-- S5: a subscript wider than N is no key a Map(FixedString(N)) can hold, so it finds nothing. That is
-- pinned by 02014_map_different_keys, 03240_array_element_or_null_for_map and
-- 01763_support_map_lowcardinality_type, and equality reads such a subscript differently, so no oracle
-- is printed beside it.
SELECT 'S5 control, wider than N, constant', m[unhex('6100000000')] FROM t_fixed_key;
SELECT 'S5 control, wider than N, non-constant', m[materialize(unhex('6100000000'))] FROM t_fixed_key;

-- S6: the constant subscript must keep reaching the padded subcolumn read rather than the matchers.
SELECT 'S6 control, subcolumn rewrite still fires', count() FROM (
    EXPLAIN QUERY TREE SELECT m[unhex('6100')] FROM t_fixed_key
) WHERE explain ILIKE '%column\_name: m.key\_%' SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE t_lc_fixed_key (m Map(LowCardinality(FixedString(4)), UInt8), k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_fixed_key SELECT map(CAST('a', 'FixedString(4)'), 7), unhex('6100');

SELECT 'S7 LowCardinality key column', m[k],
       arraySum(arrayMap((kk, v) -> if(kk = k, v, 0), mapKeys(m), mapValues(m))) FROM t_lc_fixed_key;

SELECT 'S8 LowCardinality dictionary lookup', m[unhex('6100')],
       arraySum(arrayMap((kk, v) -> if(kk = unhex('6100'), v, 0), mapKeys(m), mapValues(m)))
FROM t_lc_fixed_key SETTINGS optimize_functions_to_subcolumns = 0;

-- S9: a String key is stored at its own length, so it must keep comparing exact-length.
CREATE TABLE t_string_key (m Map(String, UInt8), k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_string_key SELECT map('a', 7), unhex('6100');

SELECT 'S9 control, String key stays exact-length', m[k],
       arraySum(arrayMap((kk, v) -> if(kk = k, v, 0), mapKeys(m), mapValues(m))) FROM t_string_key;

-- A Map key cannot be Nullable, so the LowCardinality(Nullable(FixedString)) carrier does not exist.
CREATE TABLE t_bad_key (m Map(LowCardinality(Nullable(FixedString(4))), UInt8)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The skip indexes answer for the same predicate and must agree with the function. `materialize` is
-- used deliberately: it leaves a constant in the query tree for the index while the function sees a
-- full column, which is the spelling that reaches both.
CREATE TABLE t_bloom_key (m Map(FixedString(4), UInt8), INDEX idx mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom_key SELECT map(CAST('a', 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S10 bloom_filter, indexed count matches unindexed',
       (SELECT count() FROM t_bloom_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_bloom_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

SELECT 'S11 control, bloom_filter still prunes an absent key', count() FROM t_bloom_key
WHERE m[unhex('6263')] = 7 SETTINGS force_data_skipping_indices = 'idx';

SELECT 'S12 control, bloom_filter used for an exact-width key', count() FROM t_bloom_key
WHERE m[unhex('61000000')] = 7 SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE t_text_key (m Map(FixedString(4), UInt8), INDEX idx_t mapKeys(m) TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_text_key SELECT map(CAST('a', 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S13 text index, indexed count matches unindexed',
       (SELECT count() FROM t_text_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_text_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

SELECT 'S14 control, text index still prunes an absent key', count() FROM t_text_key
WHERE m[unhex('6263')] = 7 SETTINGS force_data_skipping_indices = 'idx_t';

-- S15: the text index must be USED for a paddable key, not declined. Declining would raise
-- INDEX_NOT_USED here instead of returning every row.
SELECT 'S15 control, text index used for a paddable key', count() FROM t_text_key
WHERE m[materialize(unhex('6100'))] = 7 SETTINGS force_data_skipping_indices = 'idx_t';

-- The index element type carries the map key's LowCardinality wrapper, which has to be stripped before
-- the key width is read.
CREATE TABLE t_bloom_lc_key (m Map(LowCardinality(FixedString(4)), UInt8), INDEX idx_lc mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom_lc_key SELECT map(CAST('a', 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S16 bloom_filter over LowCardinality key, indexed matches unindexed',
       (SELECT count() FROM t_bloom_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_bloom_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

CREATE TABLE t_text_lc_key (m Map(LowCardinality(FixedString(4)), UInt8), INDEX idx_tlc mapKeys(m) TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_text_lc_key SELECT map(CAST('a', 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S17 text index over LowCardinality key, indexed matches unindexed',
       (SELECT count() FROM t_text_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_text_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

SELECT 'S18 control, text index over LowCardinality key still prunes an absent key', count() FROM t_text_lc_key
WHERE m[unhex('6263')] = 7 SETTINGS force_data_skipping_indices = 'idx_tlc';

DROP TABLE t_fixed_key;
DROP TABLE t_lc_fixed_key;
DROP TABLE t_string_key;
DROP TABLE t_bloom_key;
DROP TABLE t_text_key;
DROP TABLE t_bloom_lc_key;
DROP TABLE t_text_lc_key;
