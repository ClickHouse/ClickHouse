-- Tags: no-replicated-database
-- The `EXPLAIN` output contains database-specific identifiers.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_rewrite_like_perfect_affix = 0;

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

-- Signed probes are converted to the `UInt64` lookup type in the constant-fold rewrites.
SELECT 'declared type, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) = 'alpha';
SELECT 'narrower signed type, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', narrow) = 'beta';

-- The dictionary subquery form keeps the lookup: an empty set built at execution would skip the
-- conversion inserted into its left operand.
SELECT 'declared type, like - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%';

-- With no matching key the predicate is not folded to a constant, since that would skip the
-- conversion: the converted probe is compared with a value no signed integer converts to.
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
-- the rewrite, for a single match, several matches, no match, and the predicates that would
-- have used the dictionary subquery, whether they match a key or not.
CREATE TABLE simple_signed_negative (id Int64) ENGINE = Memory;
INSERT INTO simple_signed_negative VALUES (1), (-1);

SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'alpha'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'alpha'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'beta'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'beta'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'zz%'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) LIKE 'zz%'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'missing'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM simple_signed_negative WHERE dictGet('simple_signed', 'attr', id) = 'missing'
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

DROP TABLE simple_signed_negative;
DROP TABLE simple_signed_probes;
DROP DICTIONARY simple_signed;
DROP TABLE simple_signed_source;
