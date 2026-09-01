-- Random settings limits: optimize_use_projections=(1, None); optimize_use_implicit_projections=(1, None)

-- Every arm prints the projection-served answer next to the same query with implicit projections
-- disabled, so a wrong bound shows up as a mismatched pair rather than a value someone has to judge.

SET allow_suspicious_low_cardinality_types = 1;

SELECT '--- NULL in a nullable sorting key';

CREATE TABLE pk_null (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO pk_null SELECT if(number % 5 = 0, NULL, number) FROM numbers(500);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null;
SELECT toString(min(k)), toString((SELECT min(k) FROM pk_null SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null;
SELECT count(k) FROM pk_null;

CREATE TABLE pk_null_desc (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k DESC SETTINGS allow_nullable_key = 1;
INSERT INTO pk_null_desc SELECT if(number % 5 = 0, NULL, number) FROM numbers(500);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_desc SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_desc;

CREATE TABLE pk_null_lc (k LowCardinality(Nullable(Int64))) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO pk_null_lc SELECT if(number % 5 = 0, NULL, number) FROM numbers(500);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_lc SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_lc;

CREATE TABLE pk_null_multi (k Nullable(Int64), s String) ENGINE = MergeTree ORDER BY (k, s) SETTINGS allow_nullable_key = 1;
INSERT INTO pk_null_multi SELECT if(number % 5 = 0, NULL, number), 'x' FROM numbers(500);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_multi SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_multi;

CREATE TABLE pk_null_str (k Nullable(String)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO pk_null_str SELECT if(number % 5 = 0, NULL, toString(number)) FROM numbers(100);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_str SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_str;

SELECT '--- the maximum shares a part with the unusable value';

CREATE TABLE pk_null_mixed (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES pk_null_mixed;
INSERT INTO pk_null_mixed VALUES (100),(200),(NULL);
INSERT INTO pk_null_mixed VALUES (3),(4);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_mixed SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_mixed;

CREATE TABLE pk_null_mixed_desc (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k DESC SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES pk_null_mixed_desc;
INSERT INTO pk_null_mixed_desc VALUES (100),(200),(NULL);
INSERT INTO pk_null_mixed_desc VALUES (3),(4);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_mixed_desc SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_mixed_desc;

SELECT '--- NULL nested in a tuple sorting key';

CREATE TABLE pk_tuple_null (k Tuple(Nullable(Int64), Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO pk_tuple_null VALUES ((5,3)),((1,100)),((NULL,2));
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_tuple_null SETTINGS optimize_use_implicit_projections = 0)) FROM pk_tuple_null;

CREATE TABLE pk_tuple_null_mixed (k Tuple(Nullable(Int64), Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES pk_tuple_null_mixed;
INSERT INTO pk_tuple_null_mixed VALUES ((200,2)),((NULL,9));
INSERT INTO pk_tuple_null_mixed VALUES ((4,5));
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_tuple_null_mixed SETTINGS optimize_use_implicit_projections = 0)) FROM pk_tuple_null_mixed;

SELECT '--- NaN in a float sorting key, no Nullable involved';

CREATE TABLE pk_nan (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO pk_nan VALUES (1),(200),(nan);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan;

CREATE TABLE pk_nan_mixed (k Float64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES pk_nan_mixed;
INSERT INTO pk_nan_mixed VALUES (100),(200),(nan);
INSERT INTO pk_nan_mixed VALUES (3),(4);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan_mixed SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan_mixed;

CREATE TABLE pk_nan_f32 (k Float32) ENGINE = MergeTree ORDER BY k;
INSERT INTO pk_nan_f32 VALUES (1),(200),(nan);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan_f32 SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan_f32;

CREATE TABLE pk_nan_bf16 (k BFloat16) ENGINE = MergeTree ORDER BY k;
INSERT INTO pk_nan_bf16 VALUES (1),(200),(nan);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan_bf16 SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan_bf16;

CREATE TABLE pk_nan_desc (k Float64) ENGINE = MergeTree ORDER BY k DESC;
INSERT INTO pk_nan_desc VALUES (1),(200),(nan);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan_desc SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan_desc;

CREATE TABLE pk_nan_and_null (k Nullable(Float64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO pk_nan_and_null VALUES (1),(200),(nan),(NULL);
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan_and_null SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan_and_null;

CREATE TABLE pk_tuple_nan (k Tuple(Float64, Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO pk_tuple_nan VALUES ((200,2)),((1,5)),((nan,3));
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_tuple_nan SETTINGS optimize_use_implicit_projections = 0)) FROM pk_tuple_nan;

SELECT '--- a part whose index samples only granule starts';

-- Without a final mark the index holds granule starts only, so the last entry is not the part's last
-- row and a repeated pair does not mean the part is free of ordinary values.
CREATE TABLE pk_null_no_final_mark (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k DESC
    SETTINGS allow_nullable_key = 1, index_granularity = 8192, index_granularity_bytes = 0,
             enable_mixed_granularity_parts = 0, min_bytes_for_wide_part = 0;
INSERT INTO pk_null_no_final_mark VALUES (1),(200),(NULL);
SELECT part_type, marks FROM system.parts WHERE table = 'pk_null_no_final_mark' AND database = currentDatabase() AND active;
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_null_no_final_mark SETTINGS optimize_use_implicit_projections = 0)) FROM pk_null_no_final_mark;

CREATE TABLE pk_nan_no_final_mark (k Float64) ENGINE = MergeTree ORDER BY k DESC
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             enable_mixed_granularity_parts = 0, min_bytes_for_wide_part = 0;
INSERT INTO pk_nan_no_final_mark VALUES (1),(200),(nan);
SELECT part_type, marks FROM system.parts WHERE table = 'pk_nan_no_final_mark' AND database = currentDatabase() AND active;
SELECT toString(max(k)), toString((SELECT max(k) FROM pk_nan_no_final_mark SETTINGS optimize_use_implicit_projections = 0)) FROM pk_nan_no_final_mark;

SELECT '--- computed bounds of a minmax column';

CREATE TABLE mm_null (p Nullable(Int64), v Int64)
    ENGINE = MergeTree PARTITION BY intDiv(assumeNotNull(p), 1000000) ORDER BY v
    SETTINGS allow_nullable_key = 1;
INSERT INTO mm_null VALUES (100,1),(200,2),(NULL,3);
SELECT toString(max(p)), toString((SELECT max(p) FROM mm_null SETTINGS optimize_use_implicit_projections = 0)) FROM mm_null;

CREATE TABLE mm_null_mixed (p Nullable(Int64), v Int64)
    ENGINE = MergeTree PARTITION BY intDiv(assumeNotNull(p), 1000000) ORDER BY v
    SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES mm_null_mixed;
INSERT INTO mm_null_mixed VALUES (100,1),(200,2),(NULL,3);
INSERT INTO mm_null_mixed VALUES (3,4),(4,5);
SELECT toString(max(p)), toString((SELECT max(p) FROM mm_null_mixed SETTINGS optimize_use_implicit_projections = 0)) FROM mm_null_mixed;

-- A tuple bound is assembled from each component's extreme, so it need not be any row.
-- No NULL and no NaN take part in this one, and both ends are wrong.
CREATE TABLE mm_tuple (p Tuple(Int64, Int64), v Int64)
    ENGINE = MergeTree PARTITION BY intDiv(p.2, 1000000) ORDER BY v;
INSERT INTO mm_tuple VALUES ((5,3),1),((1,100),2);
SELECT toString(max(p)), toString((SELECT max(p) FROM mm_tuple SETTINGS optimize_use_implicit_projections = 0)) FROM mm_tuple;
SELECT toString(min(p)), toString((SELECT min(p) FROM mm_tuple SETTINGS optimize_use_implicit_projections = 0)) FROM mm_tuple;

CREATE TABLE mm_tuple_null (p Tuple(Nullable(Int64), Int64), v Int64)
    ENGINE = MergeTree PARTITION BY intDiv(p.2, 1000000) ORDER BY v SETTINGS allow_nullable_key = 1;
INSERT INTO mm_tuple_null VALUES ((5,3),1),((1,100),2);
SELECT toString(max(p)), toString((SELECT max(p) FROM mm_tuple_null SETTINGS optimize_use_implicit_projections = 0)) FROM mm_tuple_null;

SELECT '--- unaffected: the projection must keep serving these';

CREATE TABLE ok_nullable_no_nulls (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO ok_nullable_no_nulls SELECT number FROM numbers(500);
SELECT max(k) FROM ok_nullable_no_nulls;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_nullable_no_nulls) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_plain (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ok_plain SELECT number FROM numbers(500);
SELECT max(k) FROM ok_plain;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_plain) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_float_no_nan (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ok_float_no_nan VALUES (1),(200);
SELECT max(k) FROM ok_float_no_nan;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_float_no_nan) WHERE explain ILIKE '%_minmax_count_projection%';

-- Real infinities are ordinary comparable values, unlike the pruning sentinel.
CREATE TABLE ok_infinity (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ok_infinity VALUES (-inf),(1),(inf);
SELECT max(k) FROM ok_infinity;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_infinity) WHERE explain ILIKE '%_minmax_count_projection%';

-- A tuple read from the primary key index is a stored row, so it stays usable.
CREATE TABLE ok_pk_tuple (k Tuple(Int64, Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO ok_pk_tuple VALUES ((5,3)),((1,100));
SELECT max(k) FROM ok_pk_tuple;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_pk_tuple) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_pk_tuple_parts (k Tuple(Int64, Int64)) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES ok_pk_tuple_parts;
INSERT INTO ok_pk_tuple_parts VALUES ((5,3));
INSERT INTO ok_pk_tuple_parts VALUES ((1,100));
SELECT max(k) FROM ok_pk_tuple_parts;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_pk_tuple_parts) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_pk_prefix (a Int64, b Int64) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO ok_pk_prefix VALUES (5,3),(1,100);
SELECT max(a) FROM ok_pk_prefix;
SELECT count() > 0 FROM (EXPLAIN SELECT max(a) FROM ok_pk_prefix) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_mm_scalar (p Int64, v Int64) ENGINE = MergeTree PARTITION BY intDiv(p, 1000000) ORDER BY v;
INSERT INTO ok_mm_scalar VALUES (200,1),(1,2);
SELECT max(p) FROM ok_mm_scalar;
SELECT count() > 0 FROM (EXPLAIN SELECT max(p) FROM ok_mm_scalar) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_nulls_own_part (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES ok_nulls_own_part;
INSERT INTO ok_nulls_own_part VALUES (1),(2);
INSERT INTO ok_nulls_own_part VALUES (NULL);
SELECT max(k) FROM ok_nulls_own_part;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_nulls_own_part) WHERE explain ILIKE '%_minmax_count_projection%';

SELECT '--- a part holding one repeated unusable value contributes nothing, so it stays served';

CREATE TABLE ok_all_null (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO ok_all_null VALUES (NULL),(NULL);
SELECT toString(max(k)) FROM ok_all_null;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_all_null) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_all_nan (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ok_all_nan VALUES (nan),(nan);
SELECT toString(max(k)) FROM ok_all_nan;
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM ok_all_nan) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_null_partition (p Nullable(Int64), v Int64) ENGINE = MergeTree PARTITION BY p ORDER BY v
    SETTINGS allow_nullable_key = 1;
INSERT INTO ok_null_partition VALUES (5,1),(6,2),(NULL,3);
SELECT toString(max(p)), toString((SELECT max(p) FROM ok_null_partition SETTINGS optimize_use_implicit_projections = 0)) FROM ok_null_partition;
SELECT count() > 0 FROM (EXPLAIN SELECT max(p) FROM ok_null_partition) WHERE explain ILIKE '%_minmax_count_projection%';

CREATE TABLE ok_all_null_partition (p Nullable(Int64), v Int64) ENGINE = MergeTree PARTITION BY p ORDER BY v
    SETTINGS allow_nullable_key = 1;
INSERT INTO ok_all_null_partition VALUES (NULL,1),(NULL,2);
SELECT toString(max(p)) FROM ok_all_null_partition;
SELECT count() > 0 FROM (EXPLAIN SELECT max(p) FROM ok_all_null_partition) WHERE explain ILIKE '%_minmax_count_projection%';

SELECT '--- only the slot that needs the unusable bound loses the projection';

SELECT count() > 0 FROM (EXPLAIN SELECT min(k) FROM pk_null) WHERE explain ILIKE '%_minmax_count_projection%';
SELECT count() > 0 FROM (EXPLAIN SELECT max(k) FROM pk_null) WHERE explain ILIKE '%_minmax_count_projection%';
