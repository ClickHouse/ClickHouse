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

-- Same state reached without choosing a name: the view's own inner name is `.inner_id.<uuid>`,
-- which always sorts below `.inner_id.metrics.<uuid>`.
DROP TABLE IF EXISTS mv;
CREATE MATERIALIZED VIEW mv ENGINE = TimeSeries AS SELECT 1 AS a;
DROP TABLE mv;
SELECT 'mv_inner_timeseries_dropped';

-- Control: a name that sorts above the inner tables' names takes the other branch of the
-- ordering predicate and always worked.
DROP TABLE IF EXISTS prom;
CREATE TABLE prom ENGINE = TimeSeries;
DROP TABLE prom;
SELECT 'control_high_sorting_name_dropped';

-- No inner tables may be left behind by any of the drops above.
SELECT count() FROM system.tables WHERE database = currentDatabase();
