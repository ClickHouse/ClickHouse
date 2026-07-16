SET enable_analyzer = 1;
SET optimize_prune_impossible_string_comparisons = 1;
SET optimize_destructure_tuple_string_comparisons = 1;
SET date_time_output_format = 'simple';
-- These passes may rewrite the LIKE conditions produced by the destructuring and change the EXPLAIN output.
SET optimize_or_like_chain = 0;
SET optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Pruning: unsigned integer, all operators';
SELECT
    toString(number) = 'hello',
    'hello' = toString(number),
    toString(number) != 'hello',
    toString(number) LIKE '%1x%',
    toString(number) NOT LIKE '%1x%',
    toString(number) ILIKE '%HeLLo%',
    toString(number) NOT ILIKE '%HeLLo%',
    position(toString(number), 'abc'),
    positionCaseInsensitive(toString(number), 'AbC')
FROM numbers(1);

SELECT '-- Not pruned: possible matches evaluate normally';
SELECT toString(number) LIKE '%1%', toString(number) = '1', position(toString(number), '1') FROM numbers(2);

SELECT '-- Pruning: CAST and :: conversions';
SELECT CAST(number, 'String') = 'hello', number::String LIKE '%x%' FROM numbers(1);

SELECT '-- Pruning: signed integer, decimal, float';
SELECT
    toString(toInt64(number) - 5) LIKE '%x%',
    toString(toInt64(number) - 5) LIKE '%-%',
    toString(toDecimal32(number, 2)) LIKE '%x%',
    toString(number / 3) LIKE '%hello%',
    toString(1 / materialize(0.0)) LIKE '%inf%'
FROM numbers(1);

SELECT '-- Date and DateTime: the scalar conversion always renders the simple format';
SELECT
    toString(toDate('2024-01-02') + number) LIKE '%T%',
    toString(materialize(toDateTime('2024-01-02 03:04:05', 'UTC'))) LIKE '%T%',
    toString(materialize(toDateTime('2024-01-02 03:04:05', 'UTC'))) LIKE '%:%'
FROM numbers(1);

SELECT '-- DateTime inside composites honors date_time_output_format';
SET date_time_output_format = 'iso';
SELECT
    toString(materialize(toDateTime('2024-01-02 03:04:05', 'UTC'))) LIKE '%T%',
    toString(materialize([toDateTime('2024-01-02 03:04:05', 'UTC')])) LIKE '%T%'
FROM numbers(1);
SET date_time_output_format = 'unix_timestamp';
SELECT
    toString(materialize(toDateTime('2024-01-02 03:04:05', 'UTC'))) LIKE '%:%',
    toString(materialize([toDateTime('2024-01-02 03:04:05', 'UTC')])) LIKE '%:%'
FROM numbers(1);
SET date_time_output_format = 'simple';

SELECT '-- Top-level Nullable is not optimized: NULL is preserved';
SELECT
    toString(materialize(CAST(NULL, 'Nullable(UInt64)'))) = 'hello',
    toString(materialize(CAST(1, 'Nullable(UInt64)'))) = 'hello';

SELECT '-- Nullable inside a composite adds the NULL characters';
SELECT
    toString(materialize([CAST(NULL, 'Nullable(UInt64)'), CAST(1, 'Nullable(UInt64)')])) LIKE '%NULL%',
    toString(materialize([CAST(NULL, 'Nullable(UInt64)')])) LIKE '%x%';

SELECT '-- Bool and String types are not analyzed';
SELECT
    toString(materialize(true)) = 'true',
    toString(materialize('hello')) LIKE '%hello%';

SELECT '-- Composite separators are possible characters';
SELECT
    toString(materialize([1, 2])) LIKE '%[%',
    toString(materialize([1, 2])) LIKE '%;%',
    toString(materialize((1, 2))) LIKE '%(%',
    toString(materialize((1, 2))) LIKE '%;%',
    toString(materialize(map('k', 1))) LIKE '%{%',
    toString(materialize(map(1, 2))) LIKE '%;%';

SELECT '-- Pruned expression becomes a constant in the query tree';
EXPLAIN QUERY TREE SELECT count() FROM numbers(10) WHERE toString(number) LIKE '%hello%';

SELECT '-- Destructuring: tuple of a String and a number';
EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('hello world') AS s, materialize(42) AS n) WHERE toString((s, n)) ILIKE '%hello%';

SELECT '-- Destructuring results';
SELECT
    toString((s, n)) ILIKE '%hello%',
    toString((s, n)) LIKE '%42%',
    toString((s, n)) LIKE '%world 1%'
FROM (SELECT concat('hello world ', toString(number)) AS s, number + 42 AS n FROM numbers(2));

SELECT '-- Needles that could match the escaping of String elements are not destructured (and stay correct)';
SELECT
    toString((materialize('a\nb'), 1)) LIKE '%nb%',
    toString((materialize('a\nb'), 1)) LIKE '%hello%';

SELECT '-- Patterns that must not be destructured (0 = no OR chain in the query tree)';
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) LIKE '%search%phrase%');
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) LIKE '%a_b%');
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) LIKE '%a(b%');
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) LIKE 'hello%');
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) LIKE '%needle%');
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) ILIKE '%Needle%');

SELECT '-- Patterns that are destructured (1 = OR chain in the query tree)';
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize('x') AS s, materialize(1) AS n) WHERE toString((s, n)) LIKE '%hello%');
SELECT countIf(explain LIKE '%function_name: or%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize(1) AS a, materialize('x') AS s, materialize(2) AS b) WHERE toString((a, s, b)) ILIKE '%search phrase%');

SELECT '-- A tuple without String elements is pruned outright to a constant, so no OR chain is left';
SELECT countIf(explain LIKE '%function_name: or%'), countIf(explain LIKE '%function_name: like%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT materialize(1) AS a, materialize(2) AS b) WHERE toString((a, b)) LIKE '%needle%');

SELECT '-- Text index is used for global search after destructuring';
DROP TABLE IF EXISTS t_global_search;
CREATE TABLE t_global_search (id UInt64, s String, INDEX text_idx(s) TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_global_search SELECT number, concat('row ', toString(number), if(number = 777, ' uniqueword', '')) FROM numbers(1000);
SELECT count() FROM t_global_search WHERE toString(tuple(*)) LIKE '%uniqueword%';
SELECT countIf(explain LIKE '%text_idx%') >= 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_global_search WHERE toString(tuple(*)) LIKE '%uniqueword%');
DROP TABLE t_global_search;

SELECT '-- Results are identical with the optimizations disabled';
SET optimize_prune_impossible_string_comparisons = 0;
SET optimize_destructure_tuple_string_comparisons = 0;
SELECT
    toString(number) = 'hello',
    toString(number) LIKE '%1x%',
    position(toString(number), 'abc'),
    toString((concat('hello world ', toString(number)), number + 42)) ILIKE '%hello%',
    toString((materialize('a\nb'), number)) LIKE '%nb%'
FROM numbers(1);
