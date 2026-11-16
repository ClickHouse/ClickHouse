-- Tags: no-replicated-database
-- The `EXPLAIN` output contains database-specific identifiers.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;

CREATE TABLE comparison_source (k UInt16, s String, attr String) ENGINE = Memory;
INSERT INTO comparison_source VALUES (1, 'a', 'hit'), (2, 'b', 'hit'), (3, 'a', 'other');
CREATE DICTIONARY comparison_single (k UInt16, attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'comparison_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY comparison_simple (k UInt64, attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'comparison_source')) LAYOUT(HASHED()) LIFETIME(0);
CREATE DICTIONARY comparison_first (k UInt16, s String, attr String DEFAULT '')
PRIMARY KEY k, s SOURCE(CLICKHOUSE(TABLE 'comparison_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY comparison_second (s String, k UInt16, attr String DEFAULT '')
PRIMARY KEY s, k SOURCE(CLICKHOUSE(TABLE 'comparison_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE comparison_probes
(
    k UInt8,
    w UInt32,
    s String,
    narrow Tuple(UInt8, String),
    wide Tuple(UInt32, String),
    reversed Tuple(String, UInt32)
) ENGINE = Memory;
INSERT INTO comparison_probes VALUES
    (1, 1, 'a', (1, 'a'), (1, 'a'), ('a', 1)),
    (2, 2, 'b', (2, 'b'), (2, 'b'), ('b', 2)),
    (4, 4, 'a', (4, 'a'), (4, 'a'), ('a', 4));

-- Widening comparisons preserve the lookup for scalar keys, tuple calls, and tuple columns.
-- These plans also exercise SQL serialization of optimized expressions.
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_single', 'attr', tuple(k)) = 'other';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_first', 'attr', narrow) = 'other';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_simple', 'attr', w) = 'other';

-- Narrowing conversions keep the original lookup in serialized SQL for every key shape.
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_single', 'attr', tuple(w)) = 'other';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_first', 'attr', (w, s)) = 'other';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_first', 'attr', wide) = 'other';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_second', 'attr', (s, w)) = 'other';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_second', 'attr', reversed) = 'other';

-- Parsing a string key also retains the lookup, including for a simple-key dictionary.
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT k FROM comparison_probes WHERE dictGet('comparison_simple', 'attr', toString(k)) = 'other';

SELECT k,
    dictGet('comparison_single', 'attr', tuple(k)) = 'hit',
    dictGet('comparison_single', 'attr', tuple(w)) = 'hit',
    dictGet('comparison_simple', 'attr', w) = 'hit',
    dictGet('comparison_simple', 'attr', toString(k)) = 'hit',
    dictGet('comparison_first', 'attr', narrow) = 'hit',
    dictGet('comparison_first', 'attr', wide) = 'hit',
    dictGet('comparison_second', 'attr', reversed) = 'hit'
FROM comparison_probes ORDER BY k;
SELECT k,
    dictGet('comparison_single', 'attr', tuple(k)) = 'hit',
    dictGet('comparison_single', 'attr', tuple(w)) = 'hit',
    dictGet('comparison_simple', 'attr', w) = 'hit',
    dictGet('comparison_simple', 'attr', toString(k)) = 'hit',
    dictGet('comparison_first', 'attr', narrow) = 'hit',
    dictGet('comparison_first', 'attr', wide) = 'hit',
    dictGet('comparison_second', 'attr', reversed) = 'hit'
FROM comparison_probes ORDER BY k SETTINGS optimize_inverse_dictionary_lookup = 0;

-- A conversion error is preserved even when no dictionary attribute matches the predicate.
INSERT INTO comparison_probes VALUES (5, 65536, 'a', (5, 'a'), (65536, 'a'), ('a', 65536));
SELECT count() FROM comparison_probes WHERE dictGet('comparison_single', 'attr', tuple(w)) = 'missing'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM comparison_probes WHERE dictGet('comparison_first', 'attr', wide) = 'missing'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM comparison_probes WHERE dictGet('comparison_second', 'attr', reversed) = 'missing'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM comparison_probes WHERE dictGet('comparison_single', 'attr', tuple(w)) = 'missing'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM comparison_probes WHERE dictGet('comparison_first', 'attr', wide) = 'missing'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count() FROM comparison_probes WHERE dictGet('comparison_second', 'attr', reversed) = 'missing'
SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE comparison_probes;
DROP DICTIONARY comparison_second;
DROP DICTIONARY comparison_first;
DROP DICTIONARY comparison_simple;
DROP DICTIONARY comparison_single;
DROP TABLE comparison_source;
