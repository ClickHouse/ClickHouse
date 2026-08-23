-- Hash-based grouping keys rows by their raw value bytes, while sort/in-order grouping keys them
-- by `compareAt`. For floats these can disagree, because values that compare equal may have
-- different bytes.
--
-- `+0.0` and `-0.0` are canonicalized before hashing, so both ways of grouping agree on them.
-- Distinct `NaN` payloads still disagree: they compare equal, but there is no way to make hash
-- tables agree with `equals` on `NaN` values, which are not even equal to themselves.

DROP TABLE IF EXISTS test;
CREATE TABLE test (f Float64) ENGINE = MergeTree ORDER BY f;
INSERT INTO test SELECT number * 0.0 * if(number % 2 = 0, 1, -1) FROM numbers(6);

SELECT 'group by, hash', count() FROM (SELECT f FROM test GROUP BY f) SETTINGS optimize_aggregation_in_order = 0;
SELECT 'group by, in order', count() FROM (SELECT f FROM test GROUP BY f) SETTINGS optimize_aggregation_in_order = 1;

SELECT 'limit by, generic', count() FROM (SELECT f FROM (SELECT number * 0.0 * if(number % 2 = 0, 1, -1) AS f FROM numbers(6)) LIMIT 1 BY f);
SELECT 'limit by, in order', count() FROM (SELECT f FROM (SELECT number * 0.0 * if(number % 2 = 0, 1, -1) AS f FROM numbers(6)) ORDER BY f LIMIT 1 BY f);

SELECT 'negative limit by, generic', count() FROM (SELECT f FROM (SELECT number * 0.0 * if(number % 2 = 0, 1, -1) AS f FROM numbers(6)) LIMIT -1 BY f);
SELECT 'negative limit by, in order', count() FROM (SELECT f FROM (SELECT number * 0.0 * if(number % 2 = 0, 1, -1) AS f FROM numbers(6)) ORDER BY f LIMIT -1 BY f) SETTINGS query_plan_remove_redundant_sorting = 0;

DROP TABLE test;

DROP TABLE IF EXISTS test_nan;
CREATE TABLE test_nan (f Float64) ENGINE = MergeTree ORDER BY f;
-- Two quiet NaN values with different payloads.
INSERT INTO test_nan SELECT reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560) + number)) FROM numbers(2);

SELECT 'nan, hash', count() FROM (SELECT f FROM test_nan GROUP BY f) SETTINGS optimize_aggregation_in_order = 0;
SELECT 'nan, in order', count() FROM (SELECT f FROM test_nan GROUP BY f) SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE test_nan;
