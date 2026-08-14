-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A collapsed distributed plan keeps the whole query in one stage only without ORDER BY,
-- so each result below is made deterministic by a single-row predicate instead.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET make_distributed_plan = 1;

DROP TABLE IF EXISTS mt_04739;
CREATE TABLE mt_04739 (n UInt64, v UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO mt_04739 SELECT number, number * 2 FROM numbers(9);

SELECT '-- CTE read by both the outer query and the set';

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM cte WHERE number IN (SELECT number FROM cte) AND number = 4;

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM cte WHERE number GLOBAL IN (SELECT number FROM cte) AND number = 4;

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM cte WHERE number NOT IN (SELECT number FROM cte);

SELECT '-- nested set';

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM cte
WHERE number IN (SELECT number FROM cte WHERE number IN (SELECT number FROM cte)) AND number = 4;

SELECT '-- chained CTEs, one gate step per dependency level';

WITH a AS MATERIALIZED (SELECT number FROM numbers(9)),
     b AS MATERIALIZED (SELECT number FROM a)
SELECT * FROM b WHERE number IN (SELECT number FROM a) AND number = 4;

WITH a AS MATERIALIZED (SELECT number FROM numbers(9)),
     b AS MATERIALIZED (SELECT number FROM a),
     c AS MATERIALIZED (SELECT number FROM b)
SELECT * FROM c WHERE number IN (SELECT number FROM a) AND number = 4;

SELECT '-- UNION ALL';

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM (
    SELECT number FROM cte WHERE number IN (SELECT number FROM cte) AND number = 4
    UNION ALL
    SELECT number FROM cte WHERE number = 4);

SELECT '-- CTE over a MergeTree table, primary-key and non-primary-key sets';

WITH cte AS MATERIALIZED (SELECT n FROM mt_04739)
SELECT * FROM cte WHERE n IN (SELECT n FROM cte) AND n = 4;

WITH cte AS MATERIALIZED (SELECT n, v FROM mt_04739)
SELECT n FROM cte WHERE v IN (SELECT v FROM cte) AND v = 8;

SELECT '-- only one side reads the CTE';

WITH cte AS MATERIALIZED (SELECT n FROM mt_04739)
SELECT * FROM cte WHERE n IN (SELECT n FROM mt_04739) AND n = 4;

WITH cte AS MATERIALIZED (SELECT n FROM mt_04739)
SELECT n FROM mt_04739 WHERE n IN (SELECT n FROM cte) AND n = 4;

SELECT '-- unchanged without a distributed plan';

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM cte WHERE number IN (SELECT number FROM cte) AND number = 4
SETTINGS make_distributed_plan = 0;

SELECT '-- a plan that stays multi-stage is still refused';

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT count() FROM (SELECT number FROM cte UNION ALL SELECT number FROM cte); -- { serverError SUPPORT_IS_DISABLED }

WITH cte AS MATERIALIZED (SELECT number FROM numbers(9))
SELECT * FROM cte WHERE number IN (SELECT number FROM cte) ORDER BY number; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE mt_04739;
