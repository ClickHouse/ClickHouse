-- Tags: no-replicated-database
-- The `EXPLAIN` output contains database-specific identifiers.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_rewrite_like_perfect_affix = 0;

-- A simple-key dictionary is looked up as `UInt64` whatever integer type its key is declared
-- with. The rewrite compares the probe with `UInt64` key values, and it must still fire for a
-- probe of the declared signed type: the no-cast decision has to use the declared type, not the
-- lookup type, otherwise the optimization silently switches off for every simple-key dictionary
-- with a signed key (`03906_dict_case_distributed_predicate_pushdown` depends on it firing).
CREATE TABLE simple_signed_source (id Int64, attr String) ENGINE = Memory;
INSERT INTO simple_signed_source VALUES (1, 'alpha'), (2, 'beta'), (3, 'beta');
CREATE DICTIONARY simple_signed (id Int64, attr String DEFAULT '')
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'simple_signed_source')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE simple_signed_probes (id Int64, narrow Int32, s String) ENGINE = Memory;
INSERT INTO simple_signed_probes VALUES (1, 1, '1'), (2, 2, '2'), (4, 4, '4');

-- The declared type and a narrower signed type are compared without a cast, through the
-- constant fold and the dictionary subquery alike.
SELECT 'declared type, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) = 'alpha';
SELECT 'narrower signed type, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', narrow) = 'beta';
SELECT 'declared type, like - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', id) LIKE 'be%';

-- A `String` probe relies on the conversion `dictGet` performs and keeps the lookup.
SELECT 'string probe, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT id FROM simple_signed_probes WHERE dictGet('simple_signed', 'attr', s) = 'alpha';

SELECT 'results';
SELECT id,
    dictGet('simple_signed', 'attr', id) = 'alpha',
    dictGet('simple_signed', 'attr', narrow) = 'beta',
    dictGet('simple_signed', 'attr', id) LIKE 'be%',
    dictGet('simple_signed', 'attr', s) = 'alpha'
FROM simple_signed_probes ORDER BY id;
SELECT 'results, opt off';
SELECT id,
    dictGet('simple_signed', 'attr', id) = 'alpha',
    dictGet('simple_signed', 'attr', narrow) = 'beta',
    dictGet('simple_signed', 'attr', id) LIKE 'be%',
    dictGet('simple_signed', 'attr', s) = 'alpha'
FROM simple_signed_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

DROP TABLE simple_signed_probes;
DROP DICTIONARY simple_signed;
DROP TABLE simple_signed_source;
