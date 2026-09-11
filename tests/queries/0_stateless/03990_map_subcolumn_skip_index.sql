-- Tags: no-parallel-replicas
-- Test: Skip indexes (bloom_filter and text) work correctly with Map key subcolumns
-- produced by FunctionToSubcolumnsPass (m.key_<serialized_key> form).
-- When optimize_functions_to_subcolumns is enabled and Map uses with_buckets serialization,
-- m['key'] is rewritten to m.key_key. The index analysis must handle both forms.
SET explain_query_plan_default = 'legacy';

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

-- ==========================================
-- Section 1: bloom_filter index on mapKeys
-- ==========================================

DROP TABLE IF EXISTS t_map_bf_keys;

CREATE TABLE t_map_bf_keys
(
    id UInt64,
    m Map(String, String),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_bf_keys VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'}), (2, {'K2':'V2'});

SELECT '-- bloom_filter mapKeys: equals with existing key';
-- Index should prune granules: only granule 0 contains K0.
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_keys WHERE m['K0'] = 'V0'
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_bf_keys WHERE m['K0'] = 'V0' SETTINGS force_data_skipping_indices = 'idx_mk';

SELECT '-- bloom_filter mapKeys: equals with non-existing key';
SELECT id FROM t_map_bf_keys WHERE m['K9'] = 'V9' SETTINGS force_data_skipping_indices = 'idx_mk';

SELECT '-- bloom_filter mapKeys: equals with default value - index must NOT skip (safe)';
-- m['K9'] = '' should not use index because default value comparison is tricky.
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_keys WHERE m['K9'] = ''
) WHERE explain LIKE '%Granules:%';

SELECT '-- bloom_filter mapKeys: IN with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_keys WHERE m['K1'] IN ('V1', 'V9')
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_bf_keys WHERE m['K1'] IN ('V1', 'V9') SETTINGS force_data_skipping_indices = 'idx_mk';

SELECT '-- bloom_filter mapKeys: NOT IN with existing key';
SELECT id FROM t_map_bf_keys WHERE m['K0'] NOT IN ('V0') SETTINGS force_data_skipping_indices = 'idx_mk'; -- { serverError INDEX_NOT_USED }
SELECT id FROM t_map_bf_keys WHERE m['K0'] NOT IN ('V0');

SELECT '-- bloom_filter mapKeys: notEquals with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_keys WHERE m['K2'] != 'V2'
) WHERE explain LIKE '%Granules:%';

DROP TABLE t_map_bf_keys;

-- ==========================================
-- Section 2: bloom_filter index on mapValues
-- ==========================================

DROP TABLE IF EXISTS t_map_bf_vals;

CREATE TABLE t_map_bf_vals
(
    id UInt64,
    m Map(String, String),
    INDEX idx_mv mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_bf_vals VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'}), (2, {'K2':'V2'});

SELECT '-- bloom_filter mapValues: equals with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_vals WHERE m['K0'] = 'V0'
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_bf_vals WHERE m['K0'] = 'V0' SETTINGS force_data_skipping_indices = 'idx_mv';

SELECT '-- bloom_filter mapValues: IN with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_vals WHERE m['K1'] IN ('V1', 'V9')
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_bf_vals WHERE m['K1'] IN ('V1', 'V9') SETTINGS force_data_skipping_indices = 'idx_mv';

DROP TABLE t_map_bf_vals;

-- ==========================================
-- Section 3: text index on mapKeys
-- ==========================================

DROP TABLE IF EXISTS t_map_text_keys;

CREATE TABLE t_map_text_keys
(
    id UInt64,
    m Map(String, String),
    INDEX idx_tk mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_text_keys VALUES (0, {'hello world':'val0'}), (1, {'foo bar':'val1'}), (2, {'baz qux':'val2'});

SELECT '-- text index mapKeys: mapContains with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_keys WHERE mapContains(m, 'hello world')
) WHERE explain LIKE '%Granules:%';

SELECT '-- text index mapKeys: has with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_keys WHERE has(m, 'foo bar')
) WHERE explain LIKE '%Granules:%';

SELECT '-- text index mapKeys: operator[] equals - index on mapKeys used for key filtering';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_keys WHERE m['hello world'] = 'val0'
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_text_keys WHERE m['hello world'] = 'val0' SETTINGS force_data_skipping_indices = 'idx_tk';

SELECT '-- text index mapKeys: operator[] equals with non-existing key';
SELECT id FROM t_map_text_keys WHERE m['nonexistent'] = 'val0' SETTINGS force_data_skipping_indices = 'idx_tk';

SELECT '-- text index mapKeys: operator[] equals with default value - should not use index';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_keys WHERE m['nonexistent'] = ''
) WHERE explain LIKE '%Granules:%';

DROP TABLE t_map_text_keys;

-- ==========================================
-- Section 4: text index on mapValues
-- ==========================================

DROP TABLE IF EXISTS t_map_text_vals;

CREATE TABLE t_map_text_vals
(
    id UInt64,
    m Map(String, String),
    INDEX idx_tv mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_text_vals VALUES (0, {'K0':'hello world'}), (1, {'K1':'foo bar'}), (2, {'K2':'baz qux'});

SELECT '-- text index mapValues: operator[] equals with existing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_vals WHERE m['K0'] = 'hello world'
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_text_vals WHERE m['K0'] = 'hello world' SETTINGS force_data_skipping_indices = 'idx_tv';

SELECT '-- text index mapValues: operator[] equals with default value - should not use index';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_vals WHERE m['K9'] = ''
) WHERE explain LIKE '%Granules:%';

DROP TABLE t_map_text_vals;

-- ==========================================
-- Section 5: UInt64 key type with bloom_filter
-- Verify the subcolumn m.key_1 (numeric) is correctly parsed for index analysis.
-- ==========================================

DROP TABLE IF EXISTS t_map_bf_uint;

CREATE TABLE t_map_bf_uint
(
    id UInt64,
    m Map(UInt64, String),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_bf_uint VALUES (0, {100:'V0'}), (1, {200:'V1'}), (2, {300:'V2'});

SELECT '-- bloom_filter mapKeys with UInt64 key: equals';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_uint WHERE m[100] = 'V0'
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_bf_uint WHERE m[100] = 'V0' SETTINGS force_data_skipping_indices = 'idx_mk';

SELECT '-- bloom_filter mapKeys with UInt64 key: non-existing key';
SELECT id FROM t_map_bf_uint WHERE m[999] = 'V0' SETTINGS force_data_skipping_indices = 'idx_mk';

DROP TABLE t_map_bf_uint;

-- ==========================================
-- Section 6: Both mapKeys and mapValues indexes on same column
-- Verify that both indexes are used correctly with subcolumn references.
-- ==========================================

DROP TABLE IF EXISTS t_map_bf_both;

CREATE TABLE t_map_bf_both
(
    id UInt64,
    m Map(String, String),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_mv mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 1,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_bf_both VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'}), (2, {'K2':'V2'});

SELECT '-- Both indexes: equals uses keys index';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_bf_both WHERE m['K0'] = 'V0'
) WHERE explain LIKE '%Granules:%';

SELECT id FROM t_map_bf_both WHERE m['K0'] = 'V0' SETTINGS force_data_skipping_indices = 'idx_mk';

DROP TABLE t_map_bf_both;

-- ==========================================
-- Section 7: Verify correctness - same results with and without optimization
-- ==========================================

DROP TABLE IF EXISTS t_map_idx_correct;

CREATE TABLE t_map_idx_correct
(
    id UInt64,
    m Map(String, UInt64),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 8192,
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_idx_correct SELECT number, map('key' || toString(number % 3), number) FROM numbers(9);

SELECT '-- Correctness with optimization';
SELECT id, m['key0'] FROM t_map_idx_correct WHERE m['key0'] > 0 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '-- Correctness without optimization';
SELECT id, m['key0'] FROM t_map_idx_correct WHERE m['key0'] > 0 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_idx_correct;

-- ==========================================
-- Section 8: `<map>.key_<key>` names that belong to something else must not select the index
-- ==========================================
-- Index analysis used to split such a name at `.key_` and probe the map's index, while identifier
-- resolution binds the name to the shortest matching column prefix. When those disagree the index
-- prunes the granule the predicate actually matches, so each arm prints the indexed count next to
-- the `use_skip_indexes = 0` oracle: they must be equal, and equal to 1.

SELECT '-- Section 8';

DROP TABLE IF EXISTS t_shadow_dynamic;

-- A `JSON` column `j` captures `j.m.key_nokey` as a dynamic path, so the physical map `j.m` is
-- never read by this predicate.
CREATE TABLE t_shadow_dynamic
(
    j JSON,
    `j.m` Map(String, String),
    INDEX idx mapKeys(`j.m`) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_dynamic VALUES ('{"m.key_nokey":"hello"}', {'abc':'x'});

SELECT '-- dynamic JSON path shadowing a Map column: indexed count, oracle';
SELECT count(), (SELECT count() FROM t_shadow_dynamic WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 0)
FROM t_shadow_dynamic WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 1;

SELECT '-- same, with optimize_functions_to_subcolumns = 0';
SELECT count(), (SELECT count() FROM t_shadow_dynamic WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 0)
FROM t_shadow_dynamic WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_shadow_dynamic;

DROP TABLE IF EXISTS t_shadow_dynamic_bf;

-- The same shape reaching the `bloom_filter` condition instead of the text bloom filter.
CREATE TABLE t_shadow_dynamic_bf
(
    j JSON,
    `j.m` Map(String, String),
    INDEX idx mapKeys(`j.m`) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_dynamic_bf VALUES ('{"m.key_nokey":"hello"}', {'abc':'x'});

SELECT '-- bloom_filter condition: indexed count, oracle';
SELECT count(), (SELECT count() FROM t_shadow_dynamic_bf WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 0)
FROM t_shadow_dynamic_bf WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 1;

DROP TABLE t_shadow_dynamic_bf;

DROP TABLE IF EXISTS t_shadow_dynamic_text;

-- And the `text` index on `mapValues`.
CREATE TABLE t_shadow_dynamic_text
(
    j JSON,
    `j.m` Map(String, String),
    INDEX idx mapValues(`j.m`) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_dynamic_text VALUES ('{"m.key_nokey":"hello"}', {'abc':'zzz'});

SELECT '-- text index on mapValues: indexed count, oracle';
SELECT count(), (SELECT count() FROM t_shadow_dynamic_text WHERE hasToken(j.m.key_nokey, 'hello') SETTINGS use_skip_indexes = 0)
FROM t_shadow_dynamic_text WHERE hasToken(j.m.key_nokey, 'hello') SETTINGS use_skip_indexes = 1;

DROP TABLE t_shadow_dynamic_text;

DROP TABLE IF EXISTS t_shadow_physical;

-- A physical column can carry the name too.
CREATE TABLE t_shadow_physical
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_physical VALUES ({'abc':'x'}, 'hello');

SELECT '-- physical column named m.key_nokey: indexed count, oracle';
SELECT count(), (SELECT count() FROM t_shadow_physical WHERE `m.key_nokey` = 'hello' SETTINGS use_skip_indexes = 0)
FROM t_shadow_physical WHERE `m.key_nokey` = 'hello' SETTINGS use_skip_indexes = 1;

DROP TABLE t_shadow_physical;

DROP TABLE IF EXISTS t_shadow_other_map;

-- Two maps in one name space: the key belongs to the JSON's map, the index covers the physical one.
CREATE TABLE t_shadow_other_map
(
    j JSON(m Map(String, String)),
    `j.m` Map(String, String),
    INDEX idx mapKeys(`j.m`) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_other_map VALUES ('{"m":{"nokey":"hello"}}', {'abc':'x'});

SELECT '-- key of a different map under the same name space: indexed count, oracle';
SELECT count(), (SELECT count() FROM t_shadow_other_map WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 0)
FROM t_shadow_other_map WHERE j.m.key_nokey = 'hello' SETTINGS use_skip_indexes = 1;

DROP TABLE t_shadow_other_map;

DROP TABLE IF EXISTS t_shadow_same_rendered_name;

-- Two maps whose parents render to the same subcolumn name under the same storage column: the
-- direct Tuple element `x.Map(String, String)` and the `Map(String, String)` variant of element `x`.
-- Only the substream paths differ, so a name comparison cannot separate them.
SET enable_variant_type = 1;

CREATE TABLE t_shadow_same_rendered_name
(
    c Tuple(`x.Map(String, String)` Map(String, String), x Variant(Map(String, String), UInt64)),
    INDEX idx mapKeys(c.`x.Map(String, String)`) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_same_rendered_name VALUES ((map('abc', 'x'), map('nokey', 'hello')));

SELECT '-- parents rendering to the same name: indexed count, oracle';
SELECT count(), (SELECT count() FROM t_shadow_same_rendered_name WHERE c.`x.Map(String, String)`.key_nokey = 'hello' SETTINGS use_skip_indexes = 0)
FROM t_shadow_same_rendered_name WHERE c.`x.Map(String, String)`.key_nokey = 'hello' SETTINGS use_skip_indexes = 1;

DROP TABLE t_shadow_same_rendered_name;

DROP TABLE IF EXISTS t_json_typed_map;

-- In-range control: a genuine key subcolumn reached through a JSON typed path must stay indexable,
-- so the missing key still prunes and the present key is still found.
CREATE TABLE t_json_typed_map
(
    j JSON(m Map(String, String)),
    INDEX idx mapKeys(j.m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_typed_map VALUES ('{"m":{"abc":"x"}}');

SELECT '-- JSON typed path key subcolumn: index still prunes the missing key';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_typed_map WHERE j.m.key_zzz = 'x'
) WHERE explain LIKE '%Granules:%';

SELECT '-- JSON typed path key subcolumn: missing key, present key';
SELECT
    (SELECT count() FROM t_json_typed_map WHERE j.m.key_zzz = 'x'),
    (SELECT count() FROM t_json_typed_map WHERE j.m.key_abc = 'x');

DROP TABLE t_json_typed_map;
