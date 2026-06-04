-- Inconsistency: hash-based grouping keys rows by their raw value bytes, while sort/in-order
-- grouping keys them by `compareAt`. For floats these disagree: `+0.0`/`-0.0` and distinct
-- `NaN` payloads have different bytes but compare equal, so in-order operators put such keys
-- in one group while hash operators keep them separate.

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
