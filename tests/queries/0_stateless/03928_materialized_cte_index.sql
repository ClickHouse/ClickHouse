SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

CREATE TABLE tm (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO tm SELECT * FROM numbers(1000000);

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT * FROM tm WHERE x IN (SELECT c FROM t WHERE c > 5) or x IN (SELECT c FROM t WHERE c > 5)
ORDER BY x
SETTINGS force_primary_key = 1;

SELECT '--- SECOND QUERY ---';

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(5))
SELECT * FROM tm WHERE x IN (t) OR x IN (t) ORDER BY x
SETTINGS force_primary_key = 1;
