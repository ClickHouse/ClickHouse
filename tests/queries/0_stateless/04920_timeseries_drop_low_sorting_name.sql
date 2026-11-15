SET allow_experimental_time_series_table = 1;
-- The stress runner sets ignore_drop_queries_probability=0.2, which rewrites a DROP of a table
-- that keeps no data on disk into a TRUNCATE; TRUNCATE does not drop inner tables.
SET ignore_drop_queries_probability = 0;

-- A TimeSeries whose own name sorts below its inner tables' names used to self-deadlock on the
-- DDL guard, so this hung instead of returning.
DROP TABLE IF EXISTS `-ts`;
CREATE TABLE `-ts` ENGINE = TimeSeries;
DROP TABLE `-ts`;
SELECT 'low_sorting_name_dropped';

-- Master also reaches this state through `MATERIALIZED VIEW ... ENGINE = TimeSeries`, whose inner
-- name is `.inner_id.<uuid>`. That variant is master-only: there the engine ignores the columns it
-- is handed, while this branch validates them and rejects the view's own `SELECT` schema. The named
-- table above exercises the same fix.

-- Control: a name that sorts above the inner tables' names takes the other branch of the
-- ordering predicate and always worked.
DROP TABLE IF EXISTS prom;
CREATE TABLE prom ENGINE = TimeSeries;
DROP TABLE prom;
SELECT 'control_high_sorting_name_dropped';

-- No inner tables may be left behind by any of the drops above.
SELECT count() FROM system.tables WHERE database = currentDatabase();
