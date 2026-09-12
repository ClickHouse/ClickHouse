-- A FixedString(N) map key is stored zero-padded to N bytes, so a subscript of at most N bytes names
-- that same key once padded, the way equality compares the two. Each value row prints the subscript
-- answer beside an inline equality-based oracle over mapKeys/mapValues.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_fixed_key;
DROP TABLE IF EXISTS t_lc_fixed_key;
DROP TABLE IF EXISTS t_string_key;
DROP TABLE IF EXISTS t_bad_key;
DROP TABLE IF EXISTS t_bloom_key;
DROP TABLE IF EXISTS t_text_key;
DROP TABLE IF EXISTS t_bloom_lc_key;
DROP TABLE IF EXISTS t_text_lc_key;
DROP TABLE IF EXISTS t_bloom_mixed;
DROP TABLE IF EXISTS t_text_mixed;

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

-- S21: arrayElementOrNull is the same matchers in Null mode, so a paddable key must return the value
-- rather than NULL, and a subscript wider than N must keep returning NULL.
SELECT 'S21 arrayElementOrNull, key column', arrayElementOrNull(m, k) FROM t_fixed_key;

SELECT 'S23 control, arrayElementOrNull over-wide subscript is still NULL',
       arrayElementOrNull(m, unhex('6100000000')) FROM t_fixed_key;

CREATE TABLE t_lc_fixed_key (m Map(LowCardinality(FixedString(4)), UInt8), k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_fixed_key SELECT map(CAST('a', 'FixedString(4)'), 7), unhex('6100');

SELECT 'S7 LowCardinality key column', m[k],
       arraySum(arrayMap((kk, v) -> if(kk = k, v, 0), mapKeys(m), mapValues(m))) FROM t_lc_fixed_key;

SELECT 'S8 LowCardinality dictionary lookup', m[unhex('6100')],
       arraySum(arrayMap((kk, v) -> if(kk = unhex('6100'), v, 0), mapKeys(m), mapValues(m)))
FROM t_lc_fixed_key SETTINGS optimize_functions_to_subcolumns = 0;

-- S22: `optimize_functions_to_subcolumns = 0` keeps this arm on the matcher path whether or not the
-- subcolumn rewrite ever learns this spelling.
SELECT 'S22 arrayElementOrNull, LowCardinality key, constant subscript',
       arrayElementOrNull(m, unhex('6100')) FROM t_lc_fixed_key SETTINGS optimize_functions_to_subcolumns = 0;

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

-- S19: `m[k] IN (set)` resolves the key constant in a different branch of the bloom filter condition
-- (traverseTreeIn) from `m[k] = v` (traverseTreeEquals). Two set elements, so the arm cannot be folded
-- into an equality, and plain `IN`, since the branch declines `notIn`.
SELECT 'S19 bloom_filter IN path, indexed count matches unindexed',
       (SELECT count() FROM t_bloom_key WHERE m[materialize(unhex('6100'))] IN (7, 9) SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_bloom_key WHERE m[materialize(unhex('6100'))] IN (7, 9) SETTINGS use_skip_indexes = 0);

SELECT 'S20 control, bloom_filter used for the IN path', count() FROM t_bloom_key
WHERE m[materialize(unhex('6100'))] IN (7, 9) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'S26 control, bloom_filter used for a paddable key', count() FROM t_bloom_key
WHERE m[materialize(unhex('6100'))] = 7 SETTINGS force_data_skipping_indices = 'idx';

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

-- `mapKeys()` strips the key's LowCardinality wrapper, so the index element type is
-- `Array(FixedString(N))` for both carriers. These arms cross the LowCardinality map with each index
-- family: the function side resolves through the dictionary, the index side through the same padded key.
CREATE TABLE t_bloom_lc_key (m Map(LowCardinality(FixedString(4)), UInt8), INDEX idx_lc mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom_lc_key SELECT map(CAST('a', 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S16 bloom_filter over LowCardinality key, indexed matches unindexed',
       (SELECT count() FROM t_bloom_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_bloom_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

SELECT 'S27 control, bloom_filter over LowCardinality key used for a paddable key', count() FROM t_bloom_lc_key
WHERE m[materialize(unhex('6100'))] = 7 SETTINGS force_data_skipping_indices = 'idx_lc';

CREATE TABLE t_text_lc_key (m Map(LowCardinality(FixedString(4)), UInt8), INDEX idx_tlc mapKeys(m) TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_text_lc_key SELECT map(CAST('a', 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S17 text index over LowCardinality key, indexed matches unindexed',
       (SELECT count() FROM t_text_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_text_lc_key WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

SELECT 'S18 control, text index over LowCardinality key still prunes an absent key', count() FROM t_text_lc_key
WHERE m[unhex('6263')] = 7 SETTINGS force_data_skipping_indices = 'idx_tlc';

SELECT 'S28 control, text index over LowCardinality key used for a paddable key', count() FROM t_text_lc_key
WHERE m[materialize(unhex('6100'))] = 7 SETTINGS force_data_skipping_indices = 'idx_tlc';

-- S24/S25: half the granules hold a different key, so the expected count is a strict subset of the
-- table. An index that pruned the paddable key, and a subscript that matched every key, are both
-- visible here and neither is visible against single-key data.
CREATE TABLE t_bloom_mixed (m Map(FixedString(4), UInt8), INDEX idx_bm mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom_mixed SELECT map(CAST(if(number % 2 = 0, 'a', 'b'), 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S24 bloom_filter, mixed keys, indexed count matches unindexed',
       (SELECT count() FROM t_bloom_mixed WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_bloom_mixed WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

CREATE TABLE t_text_mixed (m Map(FixedString(4), UInt8), INDEX idx_tm mapKeys(m) TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_text_mixed SELECT map(CAST(if(number % 2 = 0, 'a', 'b'), 'FixedString(4)'), 7) FROM numbers(8);

SELECT 'S25 text index, mixed keys, indexed count matches unindexed',
       (SELECT count() FROM t_text_mixed WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_text_mixed WHERE m[materialize(unhex('6100'))] = 7 SETTINGS use_skip_indexes = 0);

-- S29/S30: row counts cannot see granule pruning, because the row-level predicate filters the
-- non-matching rows either way. These read the index step's own `Granules: selected/total` out of the
-- plan and assert only that a step pruned strictly between nothing and everything, so no granule count
-- is baked into the reference. A plan reports only what index analysis pruned, so the arms pin the
-- index out of the data-read path.
SELECT 'S29 bloom_filter prunes granules for a paddable key', max((sel > 0) AND (sel < tot))
FROM (
    SELECT toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) AS sel,
           toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)')) AS tot
    FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_bloom_mixed
        WHERE m[materialize(unhex('6100'))] = 7
        SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, parallel_replicas_local_plan = 1
    )
    WHERE explain LIKE '%Granules:%'
);

SELECT 'S30 text index prunes granules for a paddable key', max((sel > 0) AND (sel < tot))
FROM (
    SELECT toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) AS sel,
           toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)')) AS tot
    FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_text_mixed
        WHERE m[materialize(unhex('6100'))] = 7
        SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, parallel_replicas_local_plan = 1
    )
    WHERE explain LIKE '%Granules:%'
);

-- S31/S32: an over-wide subscript is no key the map can hold, so each index must look for its own
-- bytes and prune every granule. A pad that truncated instead would make it match the stored key,
-- which no row count can distinguish because the row-level predicate filters those rows anyway.
-- `tot > 0` drops the plan's summary counter row, which carries a bare `Granules: N` with no total.
SELECT 'S31 control, text index prunes every granule for an over-wide subscript',
       max((tot > 0) AND (sel = 0))
FROM (
    SELECT toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) AS sel,
           toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)')) AS tot
    FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_text_key
        WHERE m[materialize(unhex('6100000000'))] = 7
        SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, parallel_replicas_local_plan = 1
    )
    WHERE explain LIKE '%Granules:%'
);

SELECT 'S32 control, bloom_filter prunes every granule for an over-wide subscript',
       max((tot > 0) AND (sel = 0))
FROM (
    SELECT toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) AS sel,
           toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)')) AS tot
    FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_bloom_key
        WHERE m[materialize(unhex('6100000000'))] = 7
        SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, parallel_replicas_local_plan = 1
    )
    WHERE explain LIKE '%Granules:%'
);

-- S33/S34: an explicit `m.key_<serialized>` subcolumn is padded to N by the map type when it is read,
-- so the index must search for the same padded bytes. The `bloom_filter` family deserializes the
-- suffix through the index key type and so already agrees; the text family copies it raw.
SELECT 'S33 text index, explicit key subcolumn, indexed count matches unindexed',
       (SELECT count() FROM t_text_key WHERE m.key_a = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_text_key WHERE m.key_a = 7 SETTINGS use_skip_indexes = 0);

SELECT 'S34 control, bloom_filter explicit key subcolumn already matched unindexed',
       (SELECT count() FROM t_bloom_key WHERE m.key_a = 7 SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_bloom_key WHERE m.key_a = 7 SETTINGS use_skip_indexes = 0);

-- S35/S36: S33 compares counts on a table whose every granule holds the key, so it cannot tell a
-- padded key from an index that declined this spelling altogether - both answer 8. These use the
-- mixed-key table, where a working index selects half the granules: S35 fails with INDEX_NOT_USED if
-- the spelling is declined, and S36 reads 0 if nothing between everything and nothing was pruned.
SELECT 'S35 control, text index forced for the explicit key subcolumn', count() FROM t_text_mixed
WHERE m.key_a = 7 SETTINGS force_data_skipping_indices = 'idx_tm';

SELECT 'S36 text index prunes granules for the explicit key subcolumn', max((sel > 0) AND (sel < tot))
FROM (
    SELECT toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) AS sel,
           toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)')) AS tot
    FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_text_mixed
        WHERE m.key_a = 7
        SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, parallel_replicas_local_plan = 1
    )
    WHERE explain LIKE '%Granules:%'
);

DROP TABLE t_fixed_key;
DROP TABLE t_lc_fixed_key;
DROP TABLE t_string_key;
DROP TABLE t_bloom_key;
DROP TABLE t_text_key;
DROP TABLE t_bloom_lc_key;
DROP TABLE t_text_lc_key;
DROP TABLE t_bloom_mixed;
DROP TABLE t_text_mixed;
