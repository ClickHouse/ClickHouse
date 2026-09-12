-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: the plan assertions below read ReadFromMergeTree's own Indexes/prewhere
-- output, and use_skip_indexes_on_data_read is not supported with parallel replicas.
-- Random settings limits: query_plan_max_limit_for_top_k_optimization=(100, None)

-- Each arm prints the answer with TopK filtering ON and with it OFF. The OFF row is the ground
-- truth, so every pair below must be identical: a filter that orders NULL or NaN differently from
-- the sorter drops a row that belongs in the answer.
--
-- Fixture invariants, each load-bearing: >= 2 parts and STOP MERGES (a single part never
-- publishes a threshold); max_threads = 1 (the Float64 arms are otherwise non-deterministic);
-- and two fixture shapes, because each is vacuous where the other bites. DENSE (~1% special
-- values) exercises NULLS FIRST; SPARSE (fewer ordinary rows than the LIMIT) plus an `id DESC`
-- tie-break exercises NULLS LAST, and there every special row carries ONE identical value so the
-- rows genuinely tie and the second sort key is what decides.
--
-- The nested Dynamic arm is also fixed by #113406, which rejects the fast path on the resolved
-- comparison type.

SET max_threads = 1;
SET allow_suspicious_types_in_order_by = 1;

-- Every setting the arms depend on is pinned, because the test runner randomizes all of them and
-- passes them on the client command line, where a value of 0 would leave no filter installed and
-- make every arm below trivially agree.
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;
SET use_skip_indexes_for_top_k = 1;
SET use_skip_indexes_on_data_read = 1;
-- The cap rejects a limit above it, so a value of 1 installs no filter in the LIMIT 2 and LIMIT 3
-- arms.
SET query_plan_max_limit_for_top_k_optimization = 100;
-- The plan assertions below scan EXPLAIN text; pin the rendering (default is 'pretty' since 26.7).
SET explain_query_plan_default = 'legacy';

-- ==================== DENSE fixtures ====================

CREATE TABLE d_arr (id UInt64, v Array(Nullable(UInt64))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_arr;
INSERT INTO d_arr SELECT number, [toNullable(number + 100)] FROM numbers(0, 1000);
INSERT INTO d_arr SELECT number, [toNullable(number + 100)] FROM numbers(1000, 1000);
INSERT INTO d_arr SELECT number, if(number % 100 = 0, [NULL], [toNullable(number + 100)]) FROM numbers(2000, 1000);

CREATE TABLE d_map (id UInt64, v Map(String, Nullable(UInt64))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_map;
INSERT INTO d_map SELECT number, map('k', toNullable(number + 100)) FROM numbers(0, 1000);
INSERT INTO d_map SELECT number, map('k', toNullable(number + 100)) FROM numbers(1000, 1000);
INSERT INTO d_map SELECT number, if(number % 100 = 0, map('k', NULL), map('k', toNullable(number + 100))) FROM numbers(2000, 1000);

-- Map and LowCardinality each traverse their children with their own forEachChild, so a float
-- reached only through one of them needs its own arm; the nullable fixtures above exercise null
-- detection instead.
CREATE TABLE d_mapf (id UInt64, v Map(String, Float64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_mapf;
INSERT INTO d_mapf SELECT number, map('k', toFloat64(number + 100)) FROM numbers(0, 1000);
INSERT INTO d_mapf SELECT number, map('k', toFloat64(number + 100)) FROM numbers(1000, 1000);
INSERT INTO d_mapf SELECT number, if(number % 100 = 0, map('k', nan), map('k', toFloat64(number + 100))) FROM numbers(2000, 1000);

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE d_lcf (id UInt64, v Array(LowCardinality(Float64))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_lcf;
INSERT INTO d_lcf SELECT number, [toLowCardinality(toFloat64(number + 100))] FROM numbers(0, 1000);
INSERT INTO d_lcf SELECT number, [toLowCardinality(toFloat64(number + 100))] FROM numbers(1000, 1000);
INSERT INTO d_lcf SELECT number, if(number % 100 = 0, [toLowCardinality(nan)], [toLowCardinality(toFloat64(number + 100))]) FROM numbers(2000, 1000);

CREATE TABLE d_lc (id UInt64, v Array(LowCardinality(Nullable(String)))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_lc;
INSERT INTO d_lc SELECT number, [toLowCardinality(toNullable(leftPad(toString(number + 100), 8, '0')))] FROM numbers(0, 1000);
INSERT INTO d_lc SELECT number, [toLowCardinality(toNullable(leftPad(toString(number + 100), 8, '0')))] FROM numbers(1000, 1000);
INSERT INTO d_lc SELECT number, if(number % 100 = 0, [CAST(NULL, 'LowCardinality(Nullable(String))')], [toLowCardinality(toNullable(leftPad(toString(number + 100), 8, '0')))]) FROM numbers(2000, 1000);

CREATE TABLE d_atn (id UInt64, v Array(Tuple(UInt64, Nullable(UInt64)))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_atn;
INSERT INTO d_atn SELECT number, [tuple(toUInt64(1), toNullable(number + 100))] FROM numbers(0, 1000);
INSERT INTO d_atn SELECT number, [tuple(toUInt64(1), toNullable(number + 100))] FROM numbers(1000, 1000);
INSERT INTO d_atn SELECT number, if(number % 100 = 0, [tuple(toUInt64(1), CAST(NULL, 'Nullable(UInt64)'))], [tuple(toUInt64(1), toNullable(number + 100))]) FROM numbers(2000, 1000);

CREATE TABLE d_f64 (id UInt64, v Float64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_f64;
INSERT INTO d_f64 SELECT number, toFloat64(number + 100) FROM numbers(0, 1000);
INSERT INTO d_f64 SELECT number, toFloat64(number + 100) FROM numbers(1000, 1000);
INSERT INTO d_f64 SELECT number, if(number % 100 = 0, nan, toFloat64(number + 100)) FROM numbers(2000, 1000);

CREATE TABLE d_f32 (id UInt64, v Float32) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_f32;
INSERT INTO d_f32 SELECT number, toFloat32(number + 100) FROM numbers(0, 1000);
INSERT INTO d_f32 SELECT number, toFloat32(number + 100) FROM numbers(1000, 1000);
INSERT INTO d_f32 SELECT number, if(number % 100 = 0, CAST(nan, 'Float32'), toFloat32(number + 100)) FROM numbers(2000, 1000);

CREATE TABLE d_bf (id UInt64, v BFloat16) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_bf;
INSERT INTO d_bf SELECT number, CAST(number + 100, 'BFloat16') FROM numbers(0, 1000);
INSERT INTO d_bf SELECT number, CAST(number + 100, 'BFloat16') FROM numbers(1000, 1000);
INSERT INTO d_bf SELECT number, if(number % 100 = 0, CAST(nan, 'BFloat16'), CAST(number + 100, 'BFloat16')) FROM numbers(2000, 1000);

CREATE TABLE d_af (id UInt64, v Array(Float64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_af;
INSERT INTO d_af SELECT number, [toFloat64(number + 100)] FROM numbers(0, 1000);
INSERT INTO d_af SELECT number, [toFloat64(number + 100)] FROM numbers(1000, 1000);
INSERT INTO d_af SELECT number, if(number % 100 = 0, [nan], [toFloat64(number + 100)]) FROM numbers(2000, 1000);

CREATE TABLE d_tf (id UInt64, v Tuple(Float64, UInt64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_tf;
INSERT INTO d_tf SELECT number, tuple(toFloat64(number + 100), toUInt64(0)) FROM numbers(0, 1000);
INSERT INTO d_tf SELECT number, tuple(toFloat64(number + 100), toUInt64(0)) FROM numbers(1000, 1000);
INSERT INTO d_tf SELECT number, if(number % 100 = 0, tuple(nan, toUInt64(0)), tuple(toFloat64(number + 100), toUInt64(0))) FROM numbers(2000, 1000);

CREATE TABLE d_json (id UInt64, v JSON(f Float64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_json;
INSERT INTO d_json SELECT number, toJSONString(map('f', toString(number + 100)))::JSON(f Float64) FROM numbers(0, 1000);
INSERT INTO d_json SELECT number, toJSONString(map('f', toString(number + 100)))::JSON(f Float64) FROM numbers(1000, 1000);
INSERT INTO d_json SELECT number, toJSONString(map('f', if(number % 100 = 0, 'nan', toString(number + 100))))::JSON(f Float64) FROM numbers(2000, 1000);

CREATE TABLE d_tdyn (id UInt64, v Tuple(Dynamic)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_tdyn;
INSERT INTO d_tdyn SELECT number, tuple(toFloat64(number + 100)::Dynamic) FROM numbers(0, 1000);
INSERT INTO d_tdyn SELECT number, tuple(toFloat64(number + 100)::Dynamic) FROM numbers(1000, 1000);
INSERT INTO d_tdyn SELECT number, if(number % 100 = 0, tuple(nan::Dynamic), tuple(toFloat64(number + 100)::Dynamic)) FROM numbers(2000, 1000);

-- One constant value per part, ascending across parts, so the DESC extremum exists only in the
-- last part read. All values share one runtime type: the ordering mismatch does not need mixed
-- alternatives.
CREATE TABLE d_dyord (id UInt64, v Tuple(Dynamic)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_dyord;
INSERT INTO d_dyord SELECT number, tuple(toUInt64(0)::Dynamic) FROM numbers(0, 1000);
INSERT INTO d_dyord SELECT number + 1000, tuple(toUInt64(100)::Dynamic) FROM numbers(0, 1000);
INSERT INTO d_dyord SELECT number + 2000, tuple(toUInt64(1000)::Dynamic) FROM numbers(0, 1000);

-- Same shape with a static element type, so a divergence in d_dyord is attributable to Dynamic
-- rather than to the fixture layout.
CREATE TABLE d_stord (id UInt64, v Tuple(UInt64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_stord;
INSERT INTO d_stord SELECT number, tuple(toUInt64(0)) FROM numbers(0, 1000);
INSERT INTO d_stord SELECT number + 1000, tuple(toUInt64(100)) FROM numbers(0, 1000);
INSERT INTO d_stord SELECT number + 2000, tuple(toUInt64(1000)) FROM numbers(0, 1000);

-- A Variant of purely static alternatives is still runtime-typed: the discriminator decides the
-- order and the Field does not carry it. Alternatives are sorted by name, so UInt64 takes the
-- lower discriminator and every UInt64 precedes every UInt8; the ASC extremum is the UInt64 in the
-- last part, while the threshold published from the first part is a numerically smaller UInt8.
SET allow_suspicious_variant_types = 1;
CREATE TABLE d_vsta (id UInt64, v Tuple(Variant(UInt64, UInt8))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES d_vsta;
INSERT INTO d_vsta SELECT number, tuple(toUInt8(3)::Variant(UInt64, UInt8)) FROM numbers(0, 1000);
INSERT INTO d_vsta SELECT number + 1000, tuple(toUInt8(4)::Variant(UInt64, UInt8)) FROM numbers(0, 1000);
INSERT INTO d_vsta SELECT number + 2000, tuple(toUInt64(900)::Variant(UInt64, UInt8)) FROM numbers(0, 1000);

-- ==================== SPARSE fixtures ====================

CREATE TABLE s_arr (id UInt64, v Array(Nullable(UInt64))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_arr;
INSERT INTO s_arr SELECT number, if(number = 0, [toNullable(toUInt64(100))], [NULL]) FROM numbers(0, 1000);
INSERT INTO s_arr SELECT number, if(number = 1000, [toNullable(toUInt64(200))], [NULL]) FROM numbers(1000, 1000);
INSERT INTO s_arr SELECT number, if(number = 2000, [toNullable(toUInt64(300))], [NULL]) FROM numbers(2000, 1000);

CREATE TABLE s_f64 (id UInt64, v Float64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_f64;
INSERT INTO s_f64 SELECT number, if(number = 0, toFloat64(50), nan) FROM numbers(0, 1000);
INSERT INTO s_f64 SELECT number, nan FROM numbers(1000, 1000);
INSERT INTO s_f64 SELECT number, nan FROM numbers(2000, 1000);

CREATE TABLE s_bf (id UInt64, v BFloat16) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_bf;
INSERT INTO s_bf SELECT number, if(number = 0, CAST(50, 'BFloat16'), CAST(nan, 'BFloat16')) FROM numbers(0, 1000);
INSERT INTO s_bf SELECT number, CAST(nan, 'BFloat16') FROM numbers(1000, 1000);
INSERT INTO s_bf SELECT number, CAST(nan, 'BFloat16') FROM numbers(2000, 1000);

CREATE TABLE s_tf (id UInt64, v Tuple(Float64, UInt64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_tf;
INSERT INTO s_tf SELECT number, if(number = 0, tuple(toFloat64(50), toUInt64(0)), tuple(nan, toUInt64(0))) FROM numbers(0, 1000);
INSERT INTO s_tf SELECT number, tuple(nan, toUInt64(0)) FROM numbers(1000, 1000);
INSERT INTO s_tf SELECT number, tuple(nan, toUInt64(0)) FROM numbers(2000, 1000);

CREATE TABLE s_af (id UInt64, v Array(Float64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_af;
INSERT INTO s_af SELECT number, if(number = 0, [toFloat64(50)], [nan]) FROM numbers(0, 1000);
INSERT INTO s_af SELECT number, [nan] FROM numbers(1000, 1000);
INSERT INTO s_af SELECT number, [nan] FROM numbers(2000, 1000);

CREATE TABLE s_atf (id UInt64, v Array(Tuple(Float64, UInt64))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_atf;
INSERT INTO s_atf SELECT number, if(number = 0, [tuple(toFloat64(50), toUInt64(0))], [tuple(nan, toUInt64(0))]) FROM numbers(0, 1000);
INSERT INTO s_atf SELECT number, [tuple(nan, toUInt64(0))] FROM numbers(1000, 1000);
INSERT INTO s_atf SELECT number, [tuple(nan, toUInt64(0))] FROM numbers(2000, 1000);

CREATE TABLE s_tdyn (id UInt64, v Tuple(Dynamic)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_tdyn;
INSERT INTO s_tdyn SELECT number, if(number = 0, tuple(toFloat64(50)::Dynamic), tuple(nan::Dynamic)) FROM numbers(0, 1000);
INSERT INTO s_tdyn SELECT number, tuple(nan::Dynamic) FROM numbers(1000, 1000);
INSERT INTO s_tdyn SELECT number, tuple(nan::Dynamic) FROM numbers(2000, 1000);

CREATE TABLE s_tvar (id UInt64, v Tuple(Variant(Float64, String))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_tvar;
INSERT INTO s_tvar SELECT number, if(number = 0, tuple(toFloat64(50)::Variant(Float64, String)), tuple(nan::Variant(Float64, String))) FROM numbers(0, 1000);
INSERT INTO s_tvar SELECT number, tuple(nan::Variant(Float64, String)) FROM numbers(1000, 1000);
INSERT INTO s_tvar SELECT number, tuple(nan::Variant(Float64, String)) FROM numbers(2000, 1000);

-- The same shape with no float alternative, so the type tree exposes String and UInt64 only.
CREATE TABLE s_tvs (id UInt64, v Tuple(Variant(String, UInt64))) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_tvs;
INSERT INTO s_tvs SELECT number, if(number = 0, tuple('aaa'::Variant(String, UInt64)), tuple(CAST(NULL, 'Variant(String, UInt64)'))) FROM numbers(0, 1000);
INSERT INTO s_tvs SELECT number, tuple(CAST(NULL, 'Variant(String, UInt64)')) FROM numbers(1000, 1000);
INSERT INTO s_tvs SELECT number, tuple(CAST(NULL, 'Variant(String, UInt64)')) FROM numbers(2000, 1000);

CREATE TABLE s_nf (id UInt64, v Nullable(Float64)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_nf;
INSERT INTO s_nf SELECT number, if(number = 0, toNullable(toFloat64(50)), toNullable(nan)) FROM numbers(0, 1000);
INSERT INTO s_nf SELECT number, if(number % 250 = 0, CAST(NULL, 'Nullable(Float64)'), toNullable(nan)) FROM numbers(1000, 1000);
INSERT INTO s_nf SELECT number, toNullable(nan) FROM numbers(2000, 1000);

CREATE TABLE s_dec (id UInt64, v Decimal64(2)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_dec;
INSERT INTO s_dec SELECT number, if(number = 0, CAST(50, 'Decimal64(2)'), CAST(77, 'Decimal64(2)')) FROM numbers(0, 1000);
INSERT INTO s_dec SELECT number, CAST(77, 'Decimal64(2)') FROM numbers(1000, 1000);
INSERT INTO s_dec SELECT number, CAST(77, 'Decimal64(2)') FROM numbers(2000, 1000);

CREATE TABLE s_ord (id UInt64, v Float64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES s_ord;
INSERT INTO s_ord SELECT number, if(number = 0, toFloat64(50), toFloat64(77)) FROM numbers(0, 1000);
INSERT INTO s_ord SELECT number, toFloat64(77) FROM numbers(1000, 1000);
INSERT INTO s_ord SELECT number, toFloat64(77) FROM numbers(2000, 1000);

-- ==================== skip-index fixtures ====================

CREATE TABLE i_f64 (id UInt64, v Float64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES i_f64;
INSERT INTO i_f64 SELECT number, toFloat64(number + 100) FROM numbers(0, 1000);
INSERT INTO i_f64 SELECT number, toFloat64(number + 100) FROM numbers(1000, 1000);
INSERT INTO i_f64 SELECT number, if(number % 100 = 0, nan, toFloat64(number + 100)) FROM numbers(2000, 1000);

CREATE TABLE i_u64 (id UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES i_u64;
INSERT INTO i_u64 SELECT number, number + 100 FROM numbers(0, 1000);
INSERT INTO i_u64 SELECT number, number + 100 FROM numbers(1000, 1000);
INSERT INTO i_u64 SELECT number, number + 100 FROM numbers(2000, 1000);

-- Three dictionaries under one wrapper: nullable, plain, and float. `w` holds the wrapper constant,
-- so nothing the arms below show for `v` or `f` is explainable by the wrapper alone. In `v` and `f`
-- the NULL and the NaN sit in the dictionary rather than at the root, which is where a root-only
-- test for either property stops looking.
CREATE TABLE i_lc (id UInt64, v LowCardinality(Nullable(UInt64)), w LowCardinality(UInt64), f LowCardinality(Float64),
                   INDEX idx_v v TYPE minmax GRANULARITY 1, INDEX idx_w w TYPE minmax GRANULARITY 1,
                   INDEX idx_f f TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES i_lc;
INSERT INTO i_lc SELECT number, toLowCardinality(toNullable(number + 100)), toLowCardinality(number + 100), toLowCardinality(toFloat64(number + 100)) FROM numbers(0, 1000);
INSERT INTO i_lc SELECT number, toLowCardinality(toNullable(number + 100)), toLowCardinality(number + 100), toLowCardinality(toFloat64(number + 100)) FROM numbers(1000, 1000);
INSERT INTO i_lc SELECT number, if(number % 100 = 0, CAST(NULL, 'LowCardinality(Nullable(UInt64))'), toLowCardinality(toNullable(number + 100))), toLowCardinality(number + 100), if(number % 100 = 0, toLowCardinality(nan), toLowCardinality(toFloat64(number + 100))) FROM numbers(2000, 1000);

-- The fixture only bites with more than one part.
SELECT 'parts', min(c) > 1 FROM (SELECT count() AS c FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table);
-- The JSON fixture must really carry NaN, not a null that formats the same way.
SELECT 'json nan rows', countIf(isNaN(v.f)) FROM d_json;

-- ==================== composite NULL, ASC NULLS FIRST ====================

SELECT 'arr ANF ON ', v FROM d_arr ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'arr ANF OFF', v FROM d_arr ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'map ANF ON ', v FROM d_map ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'map ANF OFF', v FROM d_map ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'lc  ANF ON ', v FROM d_lc ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'lc  ANF OFF', v FROM d_lc ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'atn ANF ON ', v FROM d_atn ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'atn ANF OFF', v FROM d_atn ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== composite NULL, DESC NULLS LAST ====================
-- nulls_direction is -1 here too, so this direction is broken as well; it loses every real value.

SELECT 'arr DNL ON ', v, id FROM s_arr ORDER BY v DESC NULLS LAST, id DESC LIMIT 3;
SELECT 'arr DNL OFF', v, id FROM s_arr ORDER BY v DESC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== float NaN, every direction ====================
-- NaN fails both lessOrEquals and greaterOrEquals against every value including itself, so the
-- direction does not matter here.

SELECT 'f64 ANF ON ', v FROM d_f64 ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'f64 ANF OFF', v FROM d_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'f32 ANF ON ', v FROM d_f32 ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'f32 ANF OFF', v FROM d_f32 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'bf  ANF ON ', v FROM d_bf ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'bf  ANF OFF', v FROM d_bf ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'f64 DNF ON ', v FROM d_f64 ORDER BY v DESC NULLS FIRST LIMIT 1;
SELECT 'f64 DNF OFF', v FROM d_f64 ORDER BY v DESC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'f64 ANL ON ', v, id FROM s_f64 ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'f64 ANL OFF', v, id FROM s_f64 ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'f64 DNL ON ', v, id FROM s_f64 ORDER BY v DESC NULLS LAST, id DESC LIMIT 3;
SELECT 'f64 DNL OFF', v, id FROM s_f64 ORDER BY v DESC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'bf  ANL ON ', v, id FROM s_bf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'bf  ANL OFF', v, id FROM s_bf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- A root Tuple never reaches the generic comparison: FunctionComparison dispatches it to
-- executeTuple, which composes partial per-element IEEE comparisons.
SELECT 'tup ANL ON ', v, id FROM s_tf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'tup ANL OFF', v, id FROM s_tf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'tup DNL ON ', v, id FROM s_tf ORDER BY v DESC NULLS LAST, id DESC LIMIT 3;
SELECT 'tup DNL OFF', v, id FROM s_tf ORDER BY v DESC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== float nested in a composite, ASC NULLS FIRST ====================

SELECT 'arrf ANF ON ', v FROM d_af ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'arrf ANF OFF', v FROM d_af ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'tupf ANF ON ', v FROM d_tf ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'tupf ANF OFF', v FROM d_tf ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'mapf ANF ON ', v FROM d_mapf ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'mapf ANF OFF', v FROM d_mapf ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'lcf ANF ON ', v FROM d_lcf ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'lcf ANF OFF', v FROM d_lcf ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'json ANF ON ', v FROM d_json ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'json ANF OFF', v FROM d_json ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'jsub ANF ON ', v.f FROM d_json ORDER BY v.f ASC NULLS FIRST LIMIT 1;
SELECT 'jsub ANF OFF', v.f FROM d_json ORDER BY v.f ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== nested Dynamic and Variant ====================
-- Their element types are only known at runtime, so the static type cannot be inspected for a
-- float. NULLS FIRST additionally raised a LOGICAL_ERROR because the comparison resolves to
-- Nullable(UInt8) rather than UInt8.

SELECT 'tdyn ANF ON ', v FROM d_tdyn ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'tdyn ANF OFF', v FROM d_tdyn ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'tdyn ANL ON ', v, id FROM s_tdyn ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'tdyn ANL OFF', v, id FROM s_tdyn ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'tvar ANL ON ', v, id FROM s_tvar ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'tvar ANL OFF', v, id FROM s_tvar ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'tvs ANL ON ', v, id FROM s_tvs ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'tvs ANL OFF', v, id FROM s_tvs ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- A Field threshold drops a Dynamic value's discriminator, which ColumnVariant::compareAt ranks
-- first, so a threshold ordered the wrong way rejects the DESC extremum and the answer loses the
-- largest value. The static twin must agree in every arm.
SELECT 'dyord DESC ON ', v, id FROM d_dyord ORDER BY v DESC, id DESC LIMIT 2;
SELECT 'dyord DESC OFF', v, id FROM d_dyord ORDER BY v DESC, id DESC LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'dyord ASC ON ', v, id FROM d_dyord ORDER BY v ASC, id DESC LIMIT 2;
SELECT 'dyord ASC OFF', v, id FROM d_dyord ORDER BY v ASC, id DESC LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl stord DESC ON ', v, id FROM d_stord ORDER BY v DESC, id DESC LIMIT 2;
SELECT 'ctl stord DESC OFF', v, id FROM d_stord ORDER BY v DESC, id DESC LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'vsta ASC ON ', v, id FROM d_vsta ORDER BY v ASC, id DESC LIMIT 2;
SELECT 'vsta ASC OFF', v, id FROM d_vsta ORDER BY v ASC, id DESC LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'vsta DESC ON ', v, id FROM d_vsta ORDER BY v DESC, id DESC LIMIT 2;
SELECT 'vsta DESC OFF', v, id FROM d_vsta ORDER BY v DESC, id DESC LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== skip-index granule ranking ====================
-- MinMaxGranuleItem::operator< ranks granules with a raw Field comparison, so this half is lost
-- with the dynamic filter fully off, at either value of use_skip_indexes_on_data_read.

SELECT 'skip1 ON ', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;
SELECT 'skip1 OFF', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'skip0 ON ', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0;
SELECT 'skip0 OFF', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- getExtremes reports neither the NULL nor the NaN that a NULLS FIRST answer has to return first, so
-- a dictionary holding either must not reach this path.
SELECT 'lcn ANF ON ', v FROM i_lc ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;
SELECT 'lcn ANF OFF', v FROM i_lc ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'lcfd ANF ON ', f FROM i_lc ORDER BY f ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;
SELECT 'lcfd ANF OFF', f FROM i_lc ORDER BY f ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== controls: these already agreed, and must keep agreeing ====================

-- nulls_direction is 1, which is what the generic comparison already assumes.
SELECT 'ctl arr ANL ON ', v, id FROM s_arr ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl arr ANL OFF', v, id FROM s_arr ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl arr DNF ON ', v, id FROM s_arr ORDER BY v DESC NULLS FIRST, id DESC LIMIT 3;
SELECT 'ctl arr DNF OFF', v, id FROM s_arr ORDER BY v DESC NULLS FIRST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- Array-wrapped floats compare through compareAt, which is a total order; they were correct
-- under NULLS LAST already. The root-Tuple arms above prove this fixture shape can disagree.
SELECT 'ctl arrf ANL ON ', v, id FROM s_af ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl arrf ANL OFF', v, id FROM s_af ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl atf  ANL ON ', v, id FROM s_atf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl atf  ANL OFF', v, id FROM s_atf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- A top-level Nullable already took the general path. This guards the threshold tracker, which
-- publishes NaN and NULL boundary values and is deliberately left unchanged.
SELECT 'ctl nf ANF ON ', v, id FROM s_nf ORDER BY v ASC NULLS FIRST, id DESC LIMIT 3;
SELECT 'ctl nf ANF OFF', v, id FROM s_nf ORDER BY v ASC NULLS FIRST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl nf ANL ON ', v, id FROM s_nf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl nf ANL OFF', v, id FROM s_nf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl nf DNF ON ', v, id FROM s_nf ORDER BY v DESC NULLS FIRST, id DESC LIMIT 3;
SELECT 'ctl nf DNF OFF', v, id FROM s_nf ORDER BY v DESC NULLS FIRST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl nf DNL ON ', v, id FROM s_nf ORDER BY v DESC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl nf DNL OFF', v, id FROM s_nf ORDER BY v DESC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- Domains that admit neither NULL nor NaN: raw Field order already matches ORDER BY, so the fix
-- must not route them anywhere new.
SELECT 'ctl u64 ON ', v FROM i_u64 ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'ctl u64 OFF', v FROM i_u64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'ctl dec ANL ON ', v, id FROM s_dec ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl dec ANL OFF', v, id FROM s_dec ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- Same shape with an ordinary value instead of NaN: the tie itself is not what breaks the filter.
SELECT 'ctl ord ANL ON ', v, id FROM s_ord ORDER BY v ASC NULLS LAST, id DESC LIMIT 3;
SELECT 'ctl ord ANL OFF', v, id FROM s_ord ORDER BY v ASC NULLS LAST, id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- ==================== the filter is really installed in the arms above ====================
-- actions = 1 is required: with actions = 0 the prewhere expression is not printed and the
-- assertion reads 0 even when the filter is installed.

SELECT 'pres arr', count() > 0 FROM (EXPLAIN actions = 1 SELECT v FROM d_arr ORDER BY v ASC NULLS FIRST LIMIT 1) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres f64', count() > 0 FROM (EXPLAIN actions = 1 SELECT v FROM d_f64 ORDER BY v ASC NULLS FIRST LIMIT 1) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres tup', count() > 0 FROM (EXPLAIN actions = 1 SELECT v FROM s_tf ORDER BY v ASC NULLS LAST, id DESC LIMIT 3) WHERE explain LIKE '%__topKFilter%';
-- A runtime-typed member anywhere in the sort column installs no filter, at any depth, because the
-- Field threshold cannot carry a discriminator. The static twin below must still install one, or
-- the guard is rejecting on the fixture shape instead of on the type.
SELECT 'pres tdyn', count() FROM (EXPLAIN actions = 1 SELECT v FROM s_tdyn ORDER BY v ASC NULLS LAST, id DESC LIMIT 3) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres dyord', count() FROM (EXPLAIN actions = 1 SELECT v FROM d_dyord ORDER BY v DESC, id DESC LIMIT 2) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres vsta', count() FROM (EXPLAIN actions = 1 SELECT v FROM d_vsta ORDER BY v ASC, id DESC LIMIT 2) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres stord', count() > 0 FROM (EXPLAIN actions = 1 SELECT v FROM d_stord ORDER BY v DESC, id DESC LIMIT 2) WHERE explain LIKE '%__topKFilter%';
-- A dotted sort column reaches the filter only through the alias branch that resolves it to the
-- storage column name; an unresolved name returns before any filter is installed. The type tested
-- is the sort column's own: v.f is a static Float64, so it keeps the filter even though the column
-- it is read from is a JSON. Sorting on the JSON itself does not.
-- Only the analyzer resolves a dotted name to an alias, so the filter is installed exactly when the
-- analyzer is enabled. Comparing against the setting keeps both directions asserted.
SELECT 'pres jsub', (count() > 0) = getSetting('enable_analyzer') FROM (EXPLAIN actions = 1 SELECT v.f FROM d_json ORDER BY v.f ASC NULLS FIRST LIMIT 1) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres jroot', count() FROM (EXPLAIN actions = 1 SELECT v FROM d_json ORDER BY v ASC NULLS FIRST LIMIT 1) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres off varlen', count() FROM (EXPLAIN actions = 1 SELECT v FROM d_arr ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 0) WHERE explain LIKE '%__topKFilter%';
SELECT 'pres off master', count() FROM (EXPLAIN actions = 1 SELECT v FROM d_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0) WHERE explain LIKE '%__topKFilter%';

-- A float column no longer selects a minmax index for top-K granule ranking; an integer one
-- still does. Without this the results above could be explained by the comparison change alone.
SELECT 'skipidx f64', count() FROM (EXPLAIN indexes = 1 SELECT v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1) WHERE explain ILIKE '%topk%';
SELECT 'skipidx u64', count() > 0 FROM (EXPLAIN indexes = 1 SELECT v FROM i_u64 ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1) WHERE explain ILIKE '%topk%';
-- A nullable or float dictionary is rejected while the same wrapper over a plain integer one is not,
-- so each rejection is attributable to the dictionary and not to LowCardinality. These two are the
-- only carriers here whose disqualifying property is out of reach of a root-only test.
SELECT 'skipidx lcn', count() FROM (EXPLAIN indexes = 1 SELECT v FROM i_lc ORDER BY v ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1) WHERE explain ILIKE '%topk%';
SELECT 'skipidx lcfd', count() FROM (EXPLAIN indexes = 1 SELECT f FROM i_lc ORDER BY f ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1) WHERE explain ILIKE '%topk%';
SELECT 'skipidx lcw', count() > 0 FROM (EXPLAIN indexes = 1 SELECT w FROM i_lc ORDER BY w ASC NULLS FIRST LIMIT 1 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1) WHERE explain ILIKE '%topk%';

-- ==================== the general path still rejects rows ====================
-- The arms above compare answers, so a filter that accepted every row would keep all of them
-- correct and every presence row true. These two assert that the main reader reads at most half of
-- what the prewhere readers did, which only holds while the filter rejects. Two fixture properties
-- make the inequality bite: `id` is selected instead of the sort column, because reading only the
-- sort column leaves the main reader no work at all; and half is the threshold rather than a strict
-- inequality, because a filter accepting everything still skips the final granule.

-- The two rows-read settings are pinned here rather than globally, because a global pin would
-- also change the plans the arms above assert on.
SELECT id FROM d_f64 ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS log_comment = '04899_eff_f64', use_skip_indexes_for_top_k = 0,
             use_query_condition_cache = 0,
             merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0 FORMAT Null;
SELECT id FROM d_arr ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS log_comment = '04899_eff_arr', use_skip_indexes_for_top_k = 0,
             use_query_condition_cache = 0,
             merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'eff f64', ProfileEvents['RowsReadByMainReader'] * 2 <= ProfileEvents['RowsReadByPrewhereReaders']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND event_date >= yesterday() AND event_time >= now() - 600
  AND log_comment = '04899_eff_f64' ORDER BY event_time DESC LIMIT 1;
SELECT 'eff arr', ProfileEvents['RowsReadByMainReader'] * 2 <= ProfileEvents['RowsReadByPrewhereReaders']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND event_date >= yesterday() AND event_time >= now() - 600
  AND log_comment = '04899_eff_arr' ORDER BY event_time DESC LIMIT 1;
