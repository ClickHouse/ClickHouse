SET query_plan_max_limit_for_lazy_materialization = 3,
    optimize_functions_to_subcolumns = 1,
    enable_analyzer = 1,
    parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS 03760_data, 03760_name_conflict;

CREATE TABLE 03760_data (
    key UInt32,
    dt Date,
    str String
)
ENGINE = MergeTree
ORDER BY key
AS
SELECT number, toDate('2025-01-01') + number, if(number % 2 = 0, '', 'str-' || number)
FROM numbers(10);

SELECT '-- No lazy materialization';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str = '' ORDER BY dt )
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_data WHERE str = '' ORDER BY dt;

SELECT '-- With lazy materialization';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3 )
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3;

SELECT '-- Rewrite to subcolumns disabled';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3 SETTINGS optimize_functions_to_subcolumns = 0 )
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- Mixed conditions, no lazy materialization';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str = '' OR str = 'foo' ORDER BY dt )
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_data WHERE str = '' OR str = 'foo' ORDER BY dt;

SELECT '-- Mixed conditions, with lazy materialization';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str = '' OR str = 'foo' ORDER BY dt LIMIT 3 )
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_data WHERE str = '' OR str = 'foo' ORDER BY dt LIMIT 3;

SELECT '-- Prewhere column not removed';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT *, str = '' FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT *, str = '' FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3;

SELECT '-- Prewhere column removed, expression over lazy column';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT *, str != '' FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT *, str != '' FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3;

SELECT '-- Prewhere disabled';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3 SETTINGS optimize_move_to_prewhere = 0)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE str = '' ORDER BY dt LIMIT 3 SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- Prewhere DAG output reused';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT *, str = '' FROM 03760_data WHERE str = '' AND key < 15 ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT *, str = '' FROM 03760_data WHERE str = '' AND key < 15 ORDER BY dt LIMIT 3;

SELECT '-- Lazy materialization with aliases';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT key, dt, str AS s FROM 03760_data WHERE s = '' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT key, dt, str AS s FROM 03760_data WHERE s = '' ORDER BY dt LIMIT 3;

SELECT '-- Mixed conditions with aliases';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT key, dt, str AS s FROM 03760_data WHERE s = '' OR s = 'foo' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT key, dt, str AS s FROM 03760_data WHERE s = '' OR s = 'foo' ORDER BY dt LIMIT 3;

SELECT '-- Prewhere DAG output reused with aliases';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT key, dt, str AS s, s = '' FROM 03760_data WHERE s = '' AND key < 15 ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT key, dt, str AS s, s = '' FROM 03760_data WHERE s = '' AND key < 15 ORDER BY dt LIMIT 3;

SELECT '-- notEmpty String, no lazy materializaton';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str != '' ORDER BY dt)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE str != '' ORDER BY dt;

SELECT '-- notEmpty String, with lazy materializaton';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE str != '' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE str != '' ORDER BY dt LIMIT 3;

SELECT '-- length String, no lazy materializaton';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE length(str) = 0 ORDER BY dt)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE length(str) = 0 ORDER BY dt;

SELECT '-- length String, with lazy materializaton';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE length(str) = 0 ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE length(str) = 0 ORDER BY dt LIMIT 3;

SELECT '-- length String gt, no lazy materializaton';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE length(str) > 0 ORDER BY dt)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE length(str) > 0 ORDER BY dt;

SELECT '-- length String gt, with lazy materializaton';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_data WHERE length(str) > 0 ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %'
   OR trimLeft(explain) LIKE 'Filter column: %';

SELECT * FROM 03760_data WHERE length(str) > 0 ORDER BY dt LIMIT 3;

CREATE TABLE 03760_name_conflict (
    key UInt32,
    dt Date,
    str String,
    `str.size` UInt64
)
ENGINE = MergeTree
ORDER BY key
AS
SELECT number, toDate('2025-01-01') + number, if(number % 2 = 0, '', 'str-' || number), 100
FROM numbers(10);

SELECT '-- empty String, with name conflict';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_name_conflict WHERE str = '' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_name_conflict WHERE str = '' ORDER BY dt LIMIT 3;

SELECT '-- notEmpty String, with name conflict';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_name_conflict WHERE str != '' ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_name_conflict WHERE str != '' ORDER BY dt LIMIT 3;

SELECT '-- length String, with name conflict';
SELECT trimLeft(explain)
FROM ( EXPLAIN actions = 1 SELECT * FROM 03760_name_conflict WHERE length(str) > 0 ORDER BY dt LIMIT 3)
WHERE trimLeft(explain) LIKE 'Prewhere filter column: %';

SELECT * FROM 03760_name_conflict WHERE length(str) > 0 ORDER BY dt LIMIT 3;

DROP TABLE 03760_data, 03760_name_conflict;
