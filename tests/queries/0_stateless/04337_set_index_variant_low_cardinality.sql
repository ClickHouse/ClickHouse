-- Regression test: an IN-set built from a subquery that yields a Variant type with a
-- LowCardinality member crashed with
-- "Bad cast from type DB::ColumnString to DB::ColumnLowCardinality"
-- in KeyCondition::applyDeterministicDagToColumn, reached via the prepared-set path
-- (tryPrepareSetIndexForIn -> tryPrepareSetColumnsForIndex). The set element column was
-- materialized through IColumn::convertToFullIfNeeded, which unwrapped the outer Const and
-- then recursed into the ColumnVariant's members, stripping their inner LowCardinality and
-- bypassing ColumnVariant's own convertToFullIfNeeded override. The set element type kept the
-- inner LowCardinality, so the Variant->String cast wrapper expected a ColumnLowCardinality
-- member but got a ColumnString.
-- This is the prepared-set sibling of 04329 (equals path and tuple-literal IN path) and of
-- issue #97854 (GLOBAL IN).

SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_set_variant_lc;

CREATE TABLE t_set_variant_lc (key Int, s String, INDEX i sipHash64(s) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_set_variant_lc SELECT number, toString(number) FROM numbers(16);

-- Prepared set from a subquery whose element is a Variant with a LowCardinality(String) member.
-- A String key has no common type with a Variant that lacks a String member, so this is a normal
-- type error during index analysis. It must be reported as an exception, not crash the server.
SELECT count() FROM t_set_variant_lc WHERE s IN (SELECT _CAST('5', 'Variant(Int256, LowCardinality(String))')); -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM t_set_variant_lc WHERE s IN (SELECT _CAST('5', 'Variant(LowCardinality(String))')); -- { serverError CANNOT_CONVERT_TYPE }

-- A Variant that does have a String member: the comparison is valid and index analysis must
-- produce a correct, prunable result without crashing. Skip index ON/OFF must agree.
SELECT count() FROM t_set_variant_lc WHERE s IN (SELECT _CAST('5', 'Variant(String, LowCardinality(String))'));
SELECT count() FROM t_set_variant_lc WHERE s IN (SELECT _CAST('5', 'Variant(String, LowCardinality(String))')) SETTINGS use_skip_indexes = 0;

-- The server is still alive and serving queries after the failed index analysis above.
SELECT count() FROM t_set_variant_lc WHERE s IN ('5', '7');

DROP TABLE t_set_variant_lc;

-- ColumnArray (and any container that always allocates a fresh column in
-- convertToFullColumnIfConst) must not loop forever in IColumn::convertToFullIfNeeded.
-- An array IN-set goes through Set::insertFromColumns -> convertToFullIfNeeded.
SELECT [1, 2] IN (SELECT [number, number + 1] FROM numbers(3));
SELECT [1, 2] IN (SELECT [number, number + 1] FROM numbers(2));
SELECT [3, 4] IN (SELECT materialize([number, number + 1]) FROM numbers(5));
SELECT 'still alive';
