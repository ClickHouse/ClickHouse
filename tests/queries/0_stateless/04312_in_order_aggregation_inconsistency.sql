-- Hash-based grouping keys rows by their raw value bytes, while grouping taken from the sort order
-- keys them by `compareAt`. For floats these could disagree: `+0.0`/`-0.0` have different bytes but
-- compare equal. Negative zeros are replaced with positive zeros before the keys are hashed, so every
-- plan puts the two zeros in one group and answers 1. (Distinct `NaN` payloads still differ.)

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
