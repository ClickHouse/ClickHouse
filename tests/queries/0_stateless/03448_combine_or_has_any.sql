SET enable_analyzer = 1;

DROP TABLE IF EXISTS t3448;
CREATE TABLE t3448
(
    id Int64,
    letters Array(String),
    numbers Array(Int128),
    dates Array(Date),
    times Array(DateTime64(3, 'UTC')),
    enums Array(Enum8('a' = 1, 'b' = 2, 'c' = 3)),
    lc Array(LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t3448 VALUES
    (0, [], [], [], [], [], []),
    (1, ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o'], [1], ['2020-01-01'], ['2020-01-01 00:00:00.001'], ['a'], ['a']),
    (2, ['a','i','u','e','o'], [2], ['2020-01-02'], ['2020-01-01 00:00:00.002'], ['b'], ['b']),
    (3, ['ą','ę','ó','ł','ś','ż','ź'], [300], ['2020-01-03'], ['2020-01-01 00:00:00.003'], ['c'], [NULL]),
    (4, ['あ', 'い', 'う', 'え', 'お'], [-1], ['2020-01-04'], ['2020-01-01 00:00:00.004'], ['a', 'b'], ['c', NULL]),
    (5, ['ア', 'イ', 'ウ', 'エ', 'オ'], [18446744073709551615], ['2020-01-05'], ['2020-01-01 00:00:00.005'], ['c', 'b'], ['d']),
    (6, ['i', 'é', 'è', 'ê', 'e', 'a', 'â', 'o', 'ô', 'u', 'ù', 'y', 'ë', 'œ', 'ø'], [], [], [], [], []),
    (7, ['i', 'n', 'e', 'd', 'i', 'b', 'l', 'e'], [], [], [], [], []),
    (8, ['z'], [], [], [], [], []),
    (9, ['d', 'e', 's', 'c', 'r', 'i', 'b', 'e'], [], [], [], [], []),
    (10, ['t', 'e', 's', 't'], [], [], [], [], []),
    (11, ['t', 'r', 's', 't'], [], [], [], [], []);

SELECT '-- chain on one haystack is merged, needles keep their order without duplicates';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(letters, ['o', 'u']) OR hasAny(letters, ['u']) OR hasAny(letters, ['ą']) OR hasAny(letters, ['a']) SETTINGS optimize_or_has_any_chain = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(letters, ['o', 'u']) OR hasAny(letters, ['u']) OR hasAny(letters, ['ą']) OR hasAny(letters, ['a']) SETTINGS optimize_or_has_any_chain = 1;

SELECT '-- other OR arguments keep their positions';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE id = 1 OR hasAny(letters, ['a']) OR length(letters) = 7 OR hasAny(letters, ['i']) OR empty(letters);

SELECT '-- nested ORs are flattened when calls are merged';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE (hasAny(letters, ['a']) OR length(letters) = 5) OR hasAny(letters, ['i']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR (length(letters) = 5 OR (id = 1 OR hasAny(letters, ['i'])));

SELECT '-- nested ORs are kept when nothing is merged';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE (hasAny(letters, ['a']) OR length(letters) = 5) OR hasAny(lc, ['i']);

SELECT '-- a single hasAny is left alone';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR id = 1;

SELECT '-- different haystacks form different groups';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(lc, ['b']) OR hasAny(letters, ['i']) OR hasAny(lc, ['c']);

SELECT '-- needles of different types are not merged with each other';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(numbers, [1]) OR hasAny(numbers, [300]) OR hasAny(numbers, [2]) OR hasAny(numbers, [400]);

SELECT '-- non-deterministic haystack is not merged';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT hasAny([rand() % 2], [0]) OR hasAny([rand() % 2], [1]);

SELECT '-- compatibility with 26.9 disables the rewrite';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) SETTINGS compatibility = '26.9';

SELECT '-- results are the same with and without the rewrite';
SELECT 'letters', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'letters and id', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['z']) OR id = 4 ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['z']) OR id = 4 ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'numbers', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(numbers, [18446744073709551615]) OR hasAny(numbers, [-1]) OR hasAny(numbers, [1]) OR hasAny(numbers, [300]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(numbers, [18446744073709551615]) OR hasAny(numbers, [-1]) OR hasAny(numbers, [1]) OR hasAny(numbers, [300]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'dates', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(dates, [toDate('2020-01-01')]) OR hasAny(dates, [toDate('2020-01-03'), toDate('2020-01-01')]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(dates, [toDate('2020-01-01')]) OR hasAny(dates, [toDate('2020-01-03'), toDate('2020-01-01')]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'single date', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(dates, [toDate('2020-01-02')]) OR id = 5 ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(dates, [toDate('2020-01-02')]) OR id = 5 ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'times', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(times, [toDateTime64('2020-01-01 00:00:00.001', 3, 'UTC')]) OR hasAny(times, [toDateTime64('2020-01-01 00:00:00.004', 3, 'UTC')]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(times, [toDateTime64('2020-01-01 00:00:00.001', 3, 'UTC')]) OR hasAny(times, [toDateTime64('2020-01-01 00:00:00.004', 3, 'UTC')]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'enums', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(enums, CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))')) OR hasAny(enums, CAST(['c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))')) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(enums, CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))')) OR hasAny(enums, CAST(['c'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3))')) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'nullable', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(lc, ['a', NULL]) OR hasAny(lc, ['d', NULL]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(lc, ['a', NULL]) OR hasAny(lc, ['d', NULL]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'nested', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE (hasAny(letters, ['a']) OR length(letters) = 7) OR (id = 4 OR hasAny(letters, ['z'])) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE (hasAny(letters, ['a']) OR length(letters) = 7) OR (id = 4 OR hasAny(letters, ['z'])) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'json', (SELECT hasAny(materialize(['{"a":1}'::JSON]), ['{"a":2}'::JSON]) OR hasAny(materialize(['{"a":1}'::JSON]), ['{"a":1}'::JSON, '{"a":2}'::JSON]) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT hasAny(materialize(['{"a":1}'::JSON]), ['{"a":2}'::JSON]) OR hasAny(materialize(['{"a":1}'::JSON]), ['{"a":1}'::JSON, '{"a":2}'::JSON]) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'aggregate states', (SELECT hasAny(materialize([initializeAggregation('sumState', 1::UInt64)]), [initializeAggregation('sumState', 1::UInt64)]) OR hasAny(materialize([initializeAggregation('sumState', 1::UInt64)]), [initializeAggregation('sumState', 2::UInt64)]) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT hasAny(materialize([initializeAggregation('sumState', 1::UInt64)]), [initializeAggregation('sumState', 1::UInt64)]) OR hasAny(materialize([initializeAggregation('sumState', 1::UInt64)]), [initializeAggregation('sumState', 2::UInt64)]) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'variants', (SELECT hasAny(materialize([CAST(1, 'Variant(UInt8, String)')]), [CAST('a', 'Variant(UInt8, String)')]) OR hasAny(materialize([CAST(1, 'Variant(UInt8, String)')]), [CAST(1, 'Variant(UInt8, String)')]) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT hasAny(materialize([CAST(1, 'Variant(UInt8, String)')]), [CAST('a', 'Variant(UInt8, String)')]) OR hasAny(materialize([CAST(1, 'Variant(UInt8, String)')]), [CAST(1, 'Variant(UInt8, String)')]) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'maps', (SELECT hasAny(materialize([map('x', 1)]), [map('y', 2)]) OR hasAny(materialize([map('x', 1)]), [map('x', 1)]) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT hasAny(materialize([map('x', 1)]), [map('y', 2)]) OR hasAny(materialize([map('x', 1)]), [map('x', 1)]) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
-- `0.` and `-0.` are duplicates, and `NaN` never matches
SELECT 'zeros', (SELECT hasAny(materialize([-0.]), [0.]) OR hasAny(materialize([-0.]), [-0.]) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT hasAny(materialize([-0.]), [0.]) OR hasAny(materialize([-0.]), [-0.]) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'nans', (SELECT hasAny(materialize([nan]), [nan]) OR hasAny(materialize([nan]), [nan, 1.]) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT hasAny(materialize([nan]), [nan]) OR hasAny(materialize([nan]), [nan, 1.]) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;
SELECT 'low cardinality needles', (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(letters, [toLowCardinality('a')]) OR hasAny(letters, [toLowCardinality('z')]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 0) AS off, (SELECT groupArray(id) FROM (SELECT id FROM t3448 WHERE hasAny(letters, [toLowCardinality('a')]) OR hasAny(letters, [toLowCardinality('z')]) ORDER BY id) SETTINGS optimize_or_has_any_chain = 1) AS on, off = on;

SELECT '-- typed needles keep their type after merging';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT id FROM t3448 WHERE hasAny(dates, [toDate('2020-01-01')]) OR hasAny(dates, [toDate('2020-01-03'), toDate('2020-01-01')]);

SELECT '-- with aggregation, GROUP BY keys and expressions after aggregation are not rewritten, arguments of aggregate functions are';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS r, sum(hasAny(letters, ['e']) OR hasAny(letters, ['o'])) FROM t3448 WHERE hasAny(letters, ['u']) OR hasAny(letters, ['z']) GROUP BY hasAny(letters, ['a']), hasAny(letters, ['i']) HAVING hasAny(letters, ['a']) OR hasAny(letters, ['i']) ORDER BY hasAny(letters, ['a']) OR hasAny(letters, ['i']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS k, count() FROM t3448 GROUP BY k;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t3448 HAVING hasAny(groupArray(id), [1]) OR hasAny(groupArray(id), [2]);
SELECT '-- subqueries in expressions after aggregation are rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t3448 HAVING count() IN (SELECT count() FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']));

SELECT '-- results of queries with aggregation';
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS r, count() FROM t3448 GROUP BY hasAny(letters, ['a']), hasAny(letters, ['i']) ORDER BY ALL;
SELECT count() FROM t3448 GROUP BY hasAny(letters, ['a']), hasAny(letters, ['i']) HAVING hasAny(letters, ['a']) OR hasAny(letters, ['i']) ORDER BY ALL;
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS r, count() FROM t3448 GROUP BY hasAny(letters, ['a']), hasAny(letters, ['i']) WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS r, count() FROM t3448 GROUP BY GROUPING SETS ((hasAny(letters, ['a']), hasAny(letters, ['i'])), ()) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS k, count() FROM t3448 GROUP BY k WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT count() FROM t3448 GROUP BY hasAny(letters, ['a']), hasAny(letters, ['i']) ORDER BY hasAny(letters, ['a']) OR hasAny(letters, ['i']), count();
SELECT sum(hasAny(letters, ['a']) OR hasAny(letters, ['i'])), countIf(hasAny(letters, ['a']) OR hasAny(letters, ['i'])) FROM t3448;
SELECT id, max(hasAny(letters, ['a']) OR hasAny(letters, ['i'])) OVER (PARTITION BY id % 2) FROM t3448 ORDER BY id;

SELECT '-- an expression shared by an alias between WHERE and the expressions after aggregation is not rewritten';
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR id = 3 AS k, count() FROM t3448 WHERE k GROUP BY k ORDER BY ALL;
SELECT hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['i']) AS k, count() FROM t3448 WHERE k GROUP BY k HAVING k ORDER BY ALL;
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS k, count() FROM t3448 WHERE k GROUP BY k HAVING k ORDER BY k;
SELECT (hasAny(letters, ['a']) OR hasAny(letters, ['i'])) AND id > 1 AS k, count() FROM t3448 WHERE k GROUP BY k ORDER BY ALL;
SELECT NOT (hasAny(letters, ['a']) OR hasAny(letters, ['i'])) AS k, count() FROM t3448 WHERE k GROUP BY k ORDER BY ALL;
SELECT k, count() FROM t3448 WHERE k GROUP BY (hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['i'])) AS k ORDER BY ALL;
SELECT '-- the same for an expression shared between an aggregate function argument and a GROUP BY key';
SELECT k, sum(k) FROM t3448 GROUP BY (hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['i'])) AS k ORDER BY ALL;
SELECT k, sum(k) FROM t3448 GROUP BY (hasAny(letters, ['a']) OR hasAny(letters, ['i'])) AS k ORDER BY ALL;
SELECT '-- the same for an expression from WITH used in a subquery';
WITH hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['i']) AS k SELECT k, count() FROM t3448 WHERE id IN (SELECT id FROM t3448 WHERE k) GROUP BY k ORDER BY ALL;
SELECT '-- an expression shared by an alias without aggregation is rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['i']) AS k FROM t3448 WHERE k;
SELECT id, hasAny(letters, ['a']) OR id = 3 OR hasAny(letters, ['i']) AS k FROM t3448 WHERE k ORDER BY id;

SELECT '-- distributed queries';
SELECT id, hasAny(letters, ['a']) OR hasAny(letters, ['i']) FROM remote('127.0.0.{1,2}', currentDatabase(), t3448) WHERE hasAny(letters, ['e']) OR hasAny(letters, ['z']) ORDER BY ALL;
SELECT sum(hasAny(letters, ['a']) OR hasAny(letters, ['i'])) FROM remote('127.0.0.{1,2}', currentDatabase(), t3448);
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS r, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t3448) GROUP BY hasAny(letters, ['a']), hasAny(letters, ['i']) ORDER BY ALL;
SELECT hasAny(letters, ['a']) OR hasAny(letters, ['i']) AS k, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t3448) GROUP BY k ORDER BY ALL;
SELECT hasAny(dates, [toDate('2020-01-01')]) OR hasAny(dates, [toDate('2020-01-03')]) AS k, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t3448) GROUP BY k ORDER BY ALL;
SELECT sum(hasAny(v, [CAST('a', 'Variant(UInt8, String)')]) OR hasAny(v, [CAST(1, 'Variant(UInt8, String)')])) FROM remote('127.0.0.{1,2}', view(SELECT [CAST(toUInt8(number), 'Variant(UInt8, String)')] AS v FROM numbers(4)));
SELECT n, hasAny(v, [CAST('a', 'Variant(UInt8, String)')]) OR hasAny(v, [CAST(1, 'Variant(UInt8, String)')]) FROM remote('127.0.0.{1,2}', view(SELECT number AS n, [CAST(toUInt8(number), 'Variant(UInt8, String)')] AS v FROM numbers(4))) ORDER BY ALL;
SELECT sum(hasAny(j, ['{"a":2}'::JSON]) OR hasAny(j, ['{"a":1}'::JSON, '{"a":2}'::JSON])) FROM remote('127.0.0.{1,2}', view(SELECT [('{"a":' || toString(number) || '}')::JSON] AS j FROM numbers(4)));

SELECT '-- constants whose value has no common type of the elements are sent to remote servers with a cast';
SELECT m, toTypeName(m) FROM (SELECT map('a', 1::Variant(UInt8, String), 'b', 'x'::Variant(UInt8, String)) AS m);
SELECT v, toTypeName(v) FROM (SELECT CAST([1, 'a']::Array(Variant(UInt8, String)), 'Variant(Array(Variant(UInt8, String)), UInt8)') AS v);
SELECT dummy, map('a', 1::Variant(UInt8, String), 'b', 'x'::Variant(UInt8, String)) FROM remote('127.0.0.{1,2}', system.one);
SELECT dummy, CAST([1, 'a']::Array(Variant(UInt8, String)), 'Variant(Array(Variant(UInt8, String)), UInt8)') FROM remote('127.0.0.{1,2}', system.one);

DROP TABLE t3448;
