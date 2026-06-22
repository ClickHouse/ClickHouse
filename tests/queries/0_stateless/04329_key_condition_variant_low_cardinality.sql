-- Regression test: a constant of a Variant type that has a LowCardinality member,
-- compared to a key column whose key expression is a non-monotonic deterministic
-- function (e.g. sipHash64), crashed with
-- "Bad cast from type DB::ColumnString to DB::ColumnLowCardinality"
-- in KeyCondition::applyDeterministicDagToColumn. The constant column was fully
-- materialized (stripping LowCardinality inside the Variant) while its type kept the
-- inner LowCardinality, so the Variant->String cast wrapper expected a
-- ColumnLowCardinality member but got a ColumnString.

SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_kc_variant_lc;

CREATE TABLE t_kc_variant_lc (key Int, s String, INDEX i sipHash64(s) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_kc_variant_lc VALUES (1, 'a'), (2, 'b'), (3, 'hello'), (4, 'c');

-- Variant with a LowCardinality(String) member alongside another member.
SELECT count() FROM t_kc_variant_lc WHERE equals(s, _CAST('hello', 'Variant(Int256, LowCardinality(String))'));
-- Variant whose members would collide after stripping LowCardinality (String and LowCardinality(String)).
SELECT count() FROM t_kc_variant_lc WHERE equals(s, _CAST('hello', 'Variant(String, LowCardinality(String))'));
-- Variant with a single LowCardinality(String) member.
SELECT count() FROM t_kc_variant_lc WHERE equals(s, _CAST('hello', 'Variant(LowCardinality(String))'));
-- The IN-set path goes through the same key-transform code.
SELECT count() FROM t_kc_variant_lc WHERE s IN (_CAST('hello', 'Variant(Int256, LowCardinality(String))'));
-- Skip index ON/OFF must agree (pruning must not change the result).
SELECT count() FROM t_kc_variant_lc WHERE equals(s, _CAST('hello', 'Variant(Int256, LowCardinality(String))')) SETTINGS use_skip_indexes = 0;
-- A non-matching constant must prune to an empty result.
SELECT count() FROM t_kc_variant_lc WHERE equals(s, _CAST('zzz', 'Variant(Int256, LowCardinality(String))'));

DROP TABLE t_kc_variant_lc;
