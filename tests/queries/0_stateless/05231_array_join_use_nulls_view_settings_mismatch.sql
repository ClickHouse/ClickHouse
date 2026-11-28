-- A VIEW stores the column types it had when it was created. `LEFT ARRAY JOIN` under
-- `array_join_use_nulls = 1` yields `Nullable` columns, so reading such a view under a different
-- value of the setting than it was created with must be reported (like `join_use_nulls` for `JOIN`),
-- instead of silently casting the result back to the stored non-`Nullable` schema.

DROP TABLE IF EXISTS view_arr_no_nulls;
DROP TABLE IF EXISTS view_arr_no_nulls_set;
DROP TABLE IF EXISTS view_arr_nulls_set;
DROP TABLE IF EXISTS view_arr_nulls;
DROP TABLE IF EXISTS view_arr_inner;
DROP TABLE IF EXISTS view_arr_wrapped;
DROP TABLE IF EXISTS view_arr_cte;

SET array_join_use_nulls = 0;

CREATE VIEW view_arr_no_nulls AS
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

CREATE VIEW view_arr_nulls_set AS
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x
SETTINGS array_join_use_nulls = 1;

-- An inner ARRAY JOIN drops rows with empty arrays and is never affected by the setting.
CREATE VIEW view_arr_inner AS
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) ARRAY JOIN arr AS x ORDER BY id, x;

-- The `LEFT ARRAY JOIN` is not in the top-level `SELECT` of the view, but its `Nullable` columns still reach the output.
CREATE VIEW view_arr_wrapped AS
SELECT * FROM (SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x) AS t ORDER BY id, x;

CREATE VIEW view_arr_cte AS
WITH t AS (SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x)
SELECT * FROM t ORDER BY id, x;

SET array_join_use_nulls = 1;

CREATE VIEW view_arr_nulls AS
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x;

CREATE VIEW view_arr_no_nulls_set AS
SELECT id, x FROM (SELECT 1 AS id, [10, 20] AS arr UNION ALL SELECT 2 AS id, [] AS arr) LEFT ARRAY JOIN arr AS x ORDER BY id, x
SETTINGS array_join_use_nulls = 0;

SELECT 'array_join_use_nulls = 1';

SELECT '-';
-- Created as non-Nullable, read as Nullable: the mismatch must be reported, not cast away.
SELECT * FROM view_arr_no_nulls; -- { serverError INCORRECT_QUERY }
SELECT * FROM view_arr_wrapped; -- { serverError INCORRECT_QUERY }
SELECT * FROM view_arr_cte; -- { serverError INCORRECT_QUERY }
SELECT '-';
SELECT * FROM view_arr_no_nulls_set;
SELECT '-';
SELECT * FROM view_arr_nulls_set;
SELECT '-';
SELECT * FROM view_arr_nulls;
SELECT '-';
SELECT * FROM view_arr_inner;

SET array_join_use_nulls = 0;

SELECT 'array_join_use_nulls = 0';

SELECT '-';
SELECT * FROM view_arr_no_nulls;
SELECT '-';
SELECT * FROM view_arr_wrapped;
SELECT '-';
SELECT * FROM view_arr_cte;
SELECT '-';
SELECT * FROM view_arr_no_nulls_set;
SELECT '-';
-- Created as Nullable, read as non-Nullable: widening to the stored Nullable type is lossless, so it is allowed.
SELECT * FROM view_arr_nulls_set;
SELECT '-';
SELECT * FROM view_arr_nulls;
SELECT '-';
SELECT * FROM view_arr_inner;

SELECT 'types';
SELECT toTypeName(x) FROM view_arr_no_nulls LIMIT 1;
SELECT toTypeName(x) FROM view_arr_nulls LIMIT 1;

DROP TABLE view_arr_no_nulls;
DROP TABLE view_arr_no_nulls_set;
DROP TABLE view_arr_nulls_set;
DROP TABLE view_arr_nulls;
DROP TABLE view_arr_inner;
DROP TABLE view_arr_wrapped;
DROP TABLE view_arr_cte;
