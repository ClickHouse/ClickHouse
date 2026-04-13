SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS 04097_data, 04097_data_sorted;

CREATE TABLE 04097_data
(
    key UInt64,
    version UInt64,
    value String,
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO 04097_data SELECT number % 10, number, 'val-' || number FROM numbers(100);

SELECT '-- Basic:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1 SELECT * FROM 04097_data ORDER BY version DESC LIMIT 1 BY key LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

SELECT * FROM 04097_data ORDER BY version DESC LIMIT 1 BY key LIMIT 3;

SELECT '-- Basic SELECT value:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1 SELECT value FROM 04097_data ORDER BY version DESC LIMIT 1 BY key LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

SELECT value FROM 04097_data ORDER BY version DESC LIMIT 1 BY key LIMIT 3;

SELECT '-- ORDER BY <expr>:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1 SELECT * FROM 04097_data ORDER BY version % 100 DESC LIMIT 1 BY key LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

SELECT * FROM 04097_data ORDER BY version % 100 DESC LIMIT 1 BY key LIMIT 3;

SELECT '-- LIMIT BY <expr>:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1 SELECT * FROM 04097_data ORDER BY version DESC LIMIT 1 BY key + 60 LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

SELECT * FROM 04097_data ORDER BY version DESC LIMIT 1 BY key + 60 LIMIT 3;

SELECT '-- ORDER BY <expr> LIMIT BY <expr>:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1 SELECT * FROM 04097_data ORDER BY version - 400 DESC LIMIT 1 BY key + 90 LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

SELECT * FROM 04097_data ORDER BY version - 400 DESC LIMIT 1 BY key + 90 LIMIT 3;

SELECT '-- ORDER BY <alias> LIMIT BY <alias>:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1
    WITH version - 400 AS sort_key,
         key + 90 AS limit_by_key
    SELECT * FROM 04097_data ORDER BY sort_key DESC LIMIT 1 BY limit_by_key LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

WITH version - 400 AS sort_key,
        key + 90 AS limit_by_key
SELECT * FROM 04097_data ORDER BY sort_key DESC LIMIT 1 BY limit_by_key LIMIT 3;

DROP TABLE 04097_data;

CREATE TABLE 04097_data_sorted
(
    key UInt64,
    version UInt64,
    value String,
)
ENGINE = MergeTree()
ORDER BY key;

INSERT INTO 04097_data_sorted SELECT number % 10, number, 'val-' || number FROM numbers(100);

SELECT '-- Basic sorted:';

SELECT trimLeft(explain)
FROM (
    EXPLAIN PLAN actions=1 SELECT * FROM 04097_data_sorted ORDER BY version DESC LIMIT 1 BY key LIMIT 3
)
WHERE explain LIKE '%Lazily read columns:%';

SELECT * FROM 04097_data_sorted ORDER BY version DESC LIMIT 1 BY key LIMIT 3;

DROP TABLE 04097_data_sorted;
