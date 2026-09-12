-- Tags: no-fasttest
--
-- Dots are legal in column names, so a table may declare a JSON column `j` and a physical column
-- named literally `j.some.path`. A JSONAllPaths / JSONAllValues skip index must not treat that name
-- as a path of `j`, and the path it hands the index must come from the substream path rather than
-- from the rendered name.
--
-- Arms 1-3 are one per index CONDITION CLASS, not one per index type: `tokenbf_v1` and `sparse_grams`
-- share MergeTreeConditionBloomFilterText with `ngrambf_v1`, so they add no coverage.
--
-- Arms that measure a REFUSAL assert the count only: with the name refused there is no index in the
-- plan, so asserting `Name: idx` there would pin the opposite of the rule. Arms that measure an
-- ACCEPTANCE assert `Name: idx` too, because `count()` alone also passes when the index is simply
-- absent, which is what a too-broad guard causes.

SET use_skip_indexes = 1;

-- ===========================================================================================
-- 1-3: the shadowed physical column, one arm per condition class
-- ===========================================================================================

DROP TABLE IF EXISTS t_shadow_text;
CREATE TABLE t_shadow_text (j JSON, `j.some.path` String,
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_text VALUES ('{"other":"v"}', 'hello');

SELECT '1 text equals', count() FROM t_shadow_text WHERE `j.some.path` = 'hello';
SELECT '1 text equals unindexed', count() FROM t_shadow_text WHERE `j.some.path` = 'hello' SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_shadow_ngrambf;
CREATE TABLE t_shadow_ngrambf (j JSON, `j.some.path` String,
    INDEX idx JSONAllPaths(j) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_ngrambf VALUES ('{"other":"v"}', 'hello');

SELECT '2 ngrambf equals', count() FROM t_shadow_ngrambf WHERE `j.some.path` = 'hello';

DROP TABLE IF EXISTS t_shadow_bf;
CREATE TABLE t_shadow_bf (j JSON, `j.some.path` String,
    INDEX idx JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_bf VALUES ('{"other":"v"}', 'hello');

SELECT '3 bloom_filter equals', count() FROM t_shadow_bf WHERE `j.some.path` = 'hello';
-- IN is a separate traverseTreeIn branch
SELECT '3 bloom_filter in', count() FROM t_shadow_bf WHERE `j.some.path` IN ('hello');

-- ===========================================================================================
-- 4: the JSONAllValues matcher, and the token route the direct-read path uses
-- ===========================================================================================

DROP TABLE IF EXISTS t_shadow_values;
CREATE TABLE t_shadow_values (j JSON, `j.some.path` String,
    INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_values VALUES ('{"other":"v"}', 'hello');

SELECT '4 JSONAllValues hasAnyTokens', count() FROM t_shadow_values WHERE hasAnyTokens(`j.some.path`, ['hello']);

-- ===========================================================================================
-- 5: a subcolumn OF the shadowing physical column
-- ===========================================================================================

SELECT '5 subcolumn of claimant', count() FROM t_shadow_text WHERE `j.some.path`.size = 5;

-- ===========================================================================================
-- 6a: the claimed name is a DYNAMIC path inside the claimant, declared nowhere
-- 6b/6c: genuine nested paths under the same shape must keep index and pruning
-- ===========================================================================================

DROP TABLE IF EXISTS t_nested_claimant;
CREATE TABLE t_nested_claimant (x Tuple(j JSON, `j.p` JSON),
    INDEX idx JSONAllPaths(x.j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_nested_claimant VALUES (('{"other":"v"}', '{"path":"hello"}'));

SELECT '6a dynamic path under claimant', count() FROM t_nested_claimant WHERE x.`j.p`.path = 'hello';
SELECT '6a unindexed', count() FROM t_nested_claimant WHERE x.`j.p`.path = 'hello' SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_nested_typed;
CREATE TABLE t_nested_typed (x Tuple(j JSON(a String)),
    INDEX idx JSONAllPaths(x.j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_nested_typed VALUES (('{"a":"hello"}'));
INSERT INTO t_nested_typed VALUES (('{"zzz":"other"}'));

SELECT '6b nested typed path keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nested_typed WHERE x.j.a = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_nested_dyn;
CREATE TABLE t_nested_dyn (x Tuple(j JSON),
    INDEX idx JSONAllPaths(x.j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_nested_dyn VALUES (('{"dyn":"hello"}'));
INSERT INTO t_nested_dyn VALUES (('{"zzz":"other"}'));

SELECT '6c nested dynamic path still prunes',
       countIf(trim(explain) ILIKE 'Name: idx'), countIf(trim(explain) ILIKE 'Granules: 1/2')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nested_dyn WHERE x.j.dyn = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

-- ===========================================================================================
-- 7-8: top-level genuine paths must keep index and pruning
-- ===========================================================================================

DROP TABLE IF EXISTS t_dyn;
CREATE TABLE t_dyn (j JSON,
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_dyn VALUES ('{"dyn":"hello"}');
INSERT INTO t_dyn VALUES ('{"zzz":"other"}');

SELECT '7 top-level dynamic path still prunes',
       countIf(trim(explain) ILIKE 'Name: idx'), countIf(trim(explain) ILIKE 'Granules: 1/2')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dyn WHERE j.dyn = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_typed;
CREATE TABLE t_typed (j JSON(a String),
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_typed VALUES ('{"a":"hello"}');
INSERT INTO t_typed VALUES ('{"zzz":"other"}');

SELECT '8 top-level typed path keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_typed WHERE j.a = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

-- ===========================================================================================
-- 9: the selected index column is the only one validated
--
-- The shortest textual candidate `x.j` does not own `x.`j.p`.a.b`. Re-selecting the longer owner
-- would probe it with the nested run path `a.b`, which JSONAllPaths does not emit for a path that
-- continues into a nested object, and would prune the granule holding the match.
-- bloom_filter, not text: a text index rejects more than one column.
-- ===========================================================================================

SET allow_suspicious_indices = 1;
DROP TABLE IF EXISTS t_two_json_columns;
CREATE TABLE t_two_json_columns (x Tuple(j JSON, `j.p` JSON(a JSON)),
    INDEX idx (JSONAllPaths(x.j), JSONAllPaths(x.`j.p`)) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_two_json_columns VALUES (('{"other":"v"}', '{"a":{"b":42}}'));
SET allow_suspicious_indices = 0;

SELECT '9 no re-selection', count() FROM t_two_json_columns WHERE x.`j.p`.a.b = 42;
SELECT '9 unindexed', count() FROM t_two_json_columns WHERE x.`j.p`.a.b = 42 SETTINGS use_skip_indexes = 0;

-- ===========================================================================================
-- 10: index analysis agrees with the analyzer in the declaration order where the resolver's
-- answer is the surprising one. Both counts are 0; this arm pins their AGREEMENT, not a fix.
-- ===========================================================================================

DROP TABLE IF EXISTS t_reversed_order;
CREATE TABLE t_reversed_order (x Tuple(`j.p` JSON, j JSON),
    INDEX idx JSONAllPaths(x.j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_reversed_order VALUES (('{"path":"hello"}', '{"other":"v"}'));

SELECT '10 reversed order', count() FROM t_reversed_order WHERE x.`j.p`.path = 'hello';
SELECT '10 reversed order unindexed', count() FROM t_reversed_order WHERE x.`j.p`.path = 'hello' SETTINGS use_skip_indexes = 0;

-- ===========================================================================================
-- 12-13: the path handed to the index comes from the substream path, and a tail that reads a
-- property DERIVED from the path's value cannot be answered from either index content.
-- ===========================================================================================

-- 12: a length is never a stored value, so JSONAllValues cannot hold it. The stored values are
-- 7,8,9 so that the compared 3 cannot match one of them by coincidence.
DROP TABLE IF EXISTS t_size_paths;
CREATE TABLE t_size_paths (j JSON(a Array(Int64)),
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_size_paths VALUES ('{"a":[7,8,9]}');

SELECT '12 size0 JSONAllPaths', count() FROM t_size_paths WHERE j.a.size0 = 3;

DROP TABLE IF EXISTS t_size_values;
CREATE TABLE t_size_values (j JSON(a Array(Int64)),
    INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_size_values VALUES ('{"a":[7,8,9]}');

SELECT '12 size0 JSONAllValues', count() FROM t_size_values WHERE j.a.size0 = 3;

-- 13: a null map is 1 exactly where the path is ABSENT, the opposite of what the path set models.
DROP TABLE IF EXISTS t_null_map;
CREATE TABLE t_null_map (j JSON,
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_null_map VALUES ('{"zzz":1}');

SELECT '13 null map of a hinted path', count() FROM t_null_map WHERE j.a.:String.null = 1;

-- ===========================================================================================
-- 14: the plain type hint is the shortest allowed tail and must stay indexed
-- ===========================================================================================

DROP TABLE IF EXISTS t_type_hint;
CREATE TABLE t_type_hint (j JSON,
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_type_hint VALUES ('{"a":"hello"}');
INSERT INTO t_type_hint VALUES ('{"zzz":1}');

SELECT '14 type hint', count() FROM t_type_hint WHERE j.a.:String = 'hello';
SELECT '14 type hint keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_type_hint WHERE j.a.:String = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

-- ===========================================================================================
-- 15: array flattening is a descent into the path's value. `json.p[]` is documented sugar for
-- ``json.p.:`Array(JSON)```, so this tail must keep BOTH its index and its pruning.
-- ===========================================================================================

DROP TABLE IF EXISTS t_flatten;
CREATE TABLE t_flatten (j JSON,
    INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_flatten VALUES ('{"labels":[{"name":"bug"}]}');
INSERT INTO t_flatten VALUES ('{"other":"zzz"}');

SELECT '15 flatten still prunes',
       countIf(trim(explain) ILIKE 'Name: idx'), countIf(trim(explain) ILIKE 'Granules: 1/2')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_flatten WHERE has(j.labels[].name::Array(String), 'bug'))
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;
SELECT '15 flatten count', count() FROM t_flatten WHERE has(j.labels[].name::Array(String), 'bug');

-- ===========================================================================================
-- 16: a structural descent under a TYPED COMPOSITE path. The run path is `a`/`b`/`c`, while the
-- rendered name says `a.x`/`b.String`/`c.key_k`, which JSONAllPaths never emits.
-- No Granules assertion: a typed path is emitted for every row, so nothing prunes.
-- ===========================================================================================

DROP TABLE IF EXISTS t_composite;
CREATE TABLE t_composite (j JSON(a Tuple(x Int64), b Variant(String, UInt64), c Map(String, Int64)),
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
-- the Map subcolumn spelling is `key_<serialized_key>`, so key `k` is read as `j.c.key_k`
INSERT INTO t_composite VALUES ('{"a":{"x":42},"b":"hello","c":{"k":7}}');

SELECT '16 tuple element', count() FROM t_composite WHERE j.a.x = 42;
SELECT '16 tuple element keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_composite WHERE j.a.x = 42)
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

SELECT '16 variant alternative', count() FROM t_composite WHERE j.b.String = 'hello';
SELECT '16 variant alternative keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_composite WHERE j.b.String = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

SELECT '16 map key value', count() FROM t_composite WHERE j.c.key_k = 7;
SELECT '16 map key value keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_composite WHERE j.c.key_k = 7)
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

-- ===========================================================================================
-- 17: Nullable(JSON) is accepted DDL and its genuine paths must keep index and pruning, so
-- NullableElements has to stay allowed both as a wrapper and inside a tail.
-- ===========================================================================================

DROP TABLE IF EXISTS t_nullable_json;
CREATE TABLE t_nullable_json (c Nullable(JSON),
    INDEX idx JSONAllPaths(c) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_nullable_json VALUES ('{"dyn":"hello"}');
INSERT INTO t_nullable_json VALUES ('{"zzz":"other"}');

SELECT '17 Nullable(JSON) still prunes',
       countIf(trim(explain) ILIKE 'Name: idx'), countIf(trim(explain) ILIKE 'Granules: 1/2')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nullable_json WHERE c.dyn = 'hello')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;
SELECT '17 Nullable(JSON) count', count() FROM t_nullable_json WHERE c.dyn = 'hello';

-- ===========================================================================================
-- 18: an unhinted dynamic path under a typed Array(JSON) path. `j.a.x` is Array(Dynamic), so the
-- predicate casts it; the name reaching the matcher is still `j.a.x`.
-- ===========================================================================================

DROP TABLE IF EXISTS t_array_json;
CREATE TABLE t_array_json (j JSON(a Array(JSON)),
    INDEX idx JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_array_json VALUES ('{"a":[{"x":42}]}');

SELECT '18 dynamic path under typed array', count() FROM t_array_json WHERE has(j.a.x::Array(Int64), 42);
SELECT '18 dynamic path under typed array keeps index', countIf(trim(explain) ILIKE 'Name: idx')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_array_json WHERE has(j.a.x::Array(Int64), 42))
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_shadow_text;
DROP TABLE IF EXISTS t_shadow_ngrambf;
DROP TABLE IF EXISTS t_shadow_bf;
DROP TABLE IF EXISTS t_shadow_values;
DROP TABLE IF EXISTS t_nested_claimant;
DROP TABLE IF EXISTS t_nested_typed;
DROP TABLE IF EXISTS t_nested_dyn;
DROP TABLE IF EXISTS t_dyn;
DROP TABLE IF EXISTS t_typed;
DROP TABLE IF EXISTS t_two_json_columns;
DROP TABLE IF EXISTS t_reversed_order;
DROP TABLE IF EXISTS t_size_paths;
DROP TABLE IF EXISTS t_size_values;
DROP TABLE IF EXISTS t_null_map;
DROP TABLE IF EXISTS t_type_hint;
DROP TABLE IF EXISTS t_flatten;
DROP TABLE IF EXISTS t_composite;
DROP TABLE IF EXISTS t_nullable_json;
DROP TABLE IF EXISTS t_array_json;
