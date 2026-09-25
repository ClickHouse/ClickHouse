-- { echoOn }

SET enable_analyzer = 1;

WITH RECURSIVE recursive_cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM recursive_cte WHERE n < 10)
SELECT n FROM recursive_cte;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT toUInt8(1) AS n UNION ALL SELECT toUInt8(n + 1) FROM recursive_cte WHERE n < 10)
SELECT n FROM recursive_cte;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT toUInt16(1) AS n UNION ALL SELECT toUInt8(n + 1) FROM recursive_cte WHERE n < 10)
SELECT n FROM recursive_cte;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT materialize(toUInt16(1)) AS n UNION ALL SELECT toUInt8(n + 1) FROM recursive_cte WHERE n < 10)
SELECT n FROM recursive_cte;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT toUInt16(1) AS n UNION ALL SELECT materialize(toUInt8(n + 1)) FROM recursive_cte WHERE n < 10)
SELECT n FROM recursive_cte;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT toUInt16(1) AS n, '1' AS concat UNION ALL SELECT materialize(toUInt8(n + 1)), concat || toString(n + 1) FROM recursive_cte WHERE n < 10)
SELECT n, concat FROM recursive_cte;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM recursive_cte)
SELECT n FROM recursive_cte LIMIT 5;

SELECT '--';

WITH RECURSIVE recursive_cte AS (SELECT materialize(toUInt8(1)) AS n UNION ALL SELECT materialize(toUInt8(n + 1)) FROM recursive_cte WHERE n < 10)
SELECT n FROM recursive_cte FORMAT Null SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- { echoOff }
