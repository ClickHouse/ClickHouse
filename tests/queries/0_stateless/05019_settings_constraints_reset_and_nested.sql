-- Settings constraints must be enforced on `SET name = DEFAULT` and on a `SETTINGS` clause
-- nested inside a subquery or a CTE, not only on a top-level `SETTINGS` clause.

DROP SETTINGS PROFILE IF EXISTS profile_05019;
DROP TABLE IF EXISTS t_05019;

CREATE TABLE t_05019 (tenant_id UInt32, secret String) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO t_05019 VALUES (1, 'tenant1-own'), (2, 'tenant2-secret'), (3, 'tenant3-secret');

-- `max_rows_to_read` is pinned to its own compiled-in default on purpose: resetting it is a no-op
-- and must stay allowed even though the setting is CONST.
CREATE SETTINGS PROFILE profile_05019 SETTINGS
    max_execution_time = 10 CONST,
    additional_table_filters = '{}' CONST,
    max_rows_to_read = 0 CONST,
    SQL_tenant_id = 1 CONST;

SET profile = 'profile_05019';

SELECT '-- constraint is in force on the direct forms';
SET max_execution_time = 20; -- { serverError 452 }
SELECT count() FROM t_05019 SETTINGS additional_table_filters = {'default.t_05019': '1'}; -- { serverError 452 }

SELECT '-- SET name = DEFAULT must not clear a CONST constraint';
SET max_execution_time = DEFAULT; -- { serverError 452 }
SELECT getSetting('max_execution_time');

SELECT '-- resetting a setting that already holds its default stays allowed';
SET max_rows_to_read = DEFAULT;
SELECT getSetting('max_rows_to_read');

SELECT '-- a custom setting pinned CONST must not be erasable';
SET SQL_tenant_id = DEFAULT; -- { serverError 452 }
SELECT getSetting('SQL_tenant_id');

SELECT '-- a nested SETTINGS clause is checked, in a subquery and in a CTE';
SELECT count() FROM (SELECT * FROM t_05019 SETTINGS additional_table_filters = {'default.t_05019': '1'}); -- { serverError 452 }
WITH cte AS (SELECT * FROM t_05019 SETTINGS additional_table_filters = {'default.t_05019': '1'}) SELECT count() FROM cte; -- { serverError 452 }

SELECT '-- a nested SETTINGS name = DEFAULT is checked too';
SELECT count() FROM (SELECT * FROM t_05019 SETTINGS max_execution_time = DEFAULT); -- { serverError 452 }

SELECT '-- an unconstrained nested SETTINGS clause still works';
SELECT count() FROM (SELECT * FROM t_05019 SETTINGS max_block_size = 100);

DROP TABLE t_05019;
DROP SETTINGS PROFILE profile_05019;
