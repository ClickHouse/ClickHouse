-- Tags: no-replicated-database
-- The `EXPLAIN` output contains database-specific identifiers.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_rewrite_like_perfect_affix = 0;
SET allow_suspicious_low_cardinality_types = 1;

-- A simple-key dictionary is looked up as `UInt64` whatever integer type its key is declared
-- with, and `dictGet` converts the probe to `UInt64` with an accurate cast. The rewrite must
-- still fire for a probe of the declared signed type, the common case, and it mirrors that
-- conversion with `accurateCast`, so a value the lookup cannot convert throws the same way with
-- the optimization on and off. `03906_dict_case_distributed_predicate_pushdown` depends on the
-- rewrite firing for such a dictionary.
CREATE TABLE simple_signed_source (id Int64, attr String) ENGINE = Memory;
INSERT INTO simple_signed_source VALUES (1, 'alpha'), (2, 'beta'), (3, 'beta');
CREATE DICTIONARY simple_signed (id Int64, attr String DEFAULT '')
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'simple_signed_source')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE simple_signed_probes (id Int64, narrow Int32, wide UInt32, s String) ENGINE = Memory;
INSERT INTO simple_signed_probes VALUES (1, 1, 1, '1'), (2, 2, 2, '2'), (4, 4, 4, '4');

-- Signed probes are converted to the `UInt64` lookup type, through the constant fold and the
-- dictionary subquery alike.
SELECT 'declared type, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) = 'alpha';
SELECT 'narrower signed type, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', narrow) = 'beta';
SELECT 'declared type, like - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%';

-- With no matching key the predicate folds to a constant without evaluating the probe, as for
-- any expression in it; the conversion is skipped together with the lookup.
SELECT 'declared type, no match - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) = 'missing';

-- An unsigned probe widens into `UInt64` and needs no conversion.
SELECT 'unsigned probe, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', wide) = 'alpha';

-- A `String` probe relies on a conversion the rewrite does not mirror and keeps the lookup.
SELECT 'string probe, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', s) = 'alpha';

SELECT 'results';
SELECT id,
    dictGet('simple_signed', 'attr', id) = 'alpha',
    dictGet('simple_signed', 'attr', narrow) = 'beta',
    dictGet('simple_signed', 'attr', id) LIKE 'be%',
    dictGet('simple_signed', 'attr', id) = 'missing',
    dictGet('simple_signed', 'attr', wide) = 'alpha',
    dictGet('simple_signed', 'attr', s) = 'alpha'
FROM simple_signed_probes ORDER BY id;
SELECT 'results, opt off';
SELECT id,
    dictGet('simple_signed', 'attr', id) = 'alpha',
    dictGet('simple_signed', 'attr', narrow) = 'beta',
    dictGet('simple_signed', 'attr', id) LIKE 'be%',
    dictGet('simple_signed', 'attr', id) = 'missing',
    dictGet('simple_signed', 'attr', wide) = 'alpha',
    dictGet('simple_signed', 'attr', s) = 'alpha'
FROM simple_signed_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

-- A negative probe value does not fit the `UInt64` lookup type: `dictGet` throws, and so does
-- the rewrite wherever the converted probe is evaluated, for a single match, several matches,
-- and the dictionary subquery. On a `MergeTree` table ordered by the probe, with a granule per
-- row, the converted probe is also a candidate for index analysis; the row holding the value
-- that does not convert is still read and converted.
CREATE TABLE simple_signed_negative (id Int64) ENGINE = Memory;
INSERT INTO simple_signed_negative VALUES (1), (-1);
CREATE TABLE simple_signed_negative_mt (id Int64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO simple_signed_negative_mt VALUES (-1);
INSERT INTO simple_signed_negative_mt VALUES (1);
INSERT INTO simple_signed_negative_mt VALUES (2);

SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'alpha'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'alpha'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'beta'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'beta'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }

SELECT count() FROM simple_signed_negative_mt WHERE dictGet('simple_signed', 'attr', id) = 'alpha'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative_mt WHERE dictGet('simple_signed', 'attr', id) = 'alpha'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative_mt WHERE dictGet('simple_signed', 'attr', id) = 'beta'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative_mt WHERE dictGet('simple_signed', 'attr', id) = 'beta'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative_mt WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative_mt WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }

-- When no dictionary key matches, the rewritten predicate is decided without evaluating the
-- probe: the constant fold replaces it with `0`, and `IN` over the empty set built by the
-- dictionary subquery is a constant as well. The conversion error disappears together with the
-- lookup, exactly as an error from any user-written expression in the probe does.
SELECT 'no match, negative probe';
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'missing';
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'missing'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'zz%';
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'zz%'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }

-- A `Nullable` probe is converted to `Nullable(UInt64)`: a `NULL` row is a missed lookup and the
-- predicate is `NULL` with the optimization on and off.
SELECT 'nullable probe - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', if(id = 1, id, NULL)) = 'alpha';
SELECT 'nullable probe';
SELECT id, dictGet('simple_signed', 'attr', if(id = 1, id, NULL)) = 'alpha' FROM simple_signed_probes ORDER BY id;
SELECT 'nullable probe, opt off';
SELECT id, dictGet('simple_signed', 'attr', if(id = 1, id, NULL)) = 'alpha' FROM simple_signed_probes ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- The nested column of a `Nullable` probe is converted as a whole, `NULL` rows included, by the
-- lookup and by the rewrite alike: a negative value hidden under `NULL` throws in both.
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', if(id > 0, id, NULL)) = 'alpha'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', if(id > 0, id, NULL)) = 'alpha'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }

-- A `LowCardinality(Nullable)` probe reaches `dictGet` as a full column, whose `Nullable` the
-- lookup strips, so a `NULL` row is a missed lookup there too; the rewrite converts the probe to
-- `Nullable(UInt64)` and agrees.
CREATE TABLE simple_signed_lc (id LowCardinality(Nullable(Int64))) ENGINE = Memory;
INSERT INTO simple_signed_lc VALUES (1), (NULL), (4);
SELECT 'low-cardinality nullable probe - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_lc WHERE dictGet('simple_signed', 'attr', id) = 'alpha';
SELECT 'low-cardinality nullable probe';
SELECT id, dictGet('simple_signed', 'attr', id) = 'alpha' FROM simple_signed_lc ORDER BY id;
SELECT 'low-cardinality nullable probe, opt off';
SELECT id, dictGet('simple_signed', 'attr', id) = 'alpha' FROM simple_signed_lc ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;

DROP TABLE simple_signed_lc;
DROP TABLE simple_signed_negative_mt;
DROP TABLE simple_signed_negative;
DROP TABLE simple_signed_probes;
DROP DICTIONARY simple_signed;
DROP TABLE simple_signed_source;
