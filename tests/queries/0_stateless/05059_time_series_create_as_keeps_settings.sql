-- The clause `AS <other_table>` copies the settings of the other table. A `SETTINGS` clause written
-- in the query itself doesn't replace them: the settings are merged by name, so a written setting
-- overrides the copied one, a written setting the other table doesn't have is added, and the rest
-- of the other table's settings are still copied. A setting written as `name = DEFAULT` is a mention
-- of that setting too, so the value of the other table is not copied for it.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_src;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS ts_derived;

CREATE TABLE ts_src ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job'}, store_min_time_and_max_time = 0;

SELECT '-- `AS` without a `SETTINGS` clause: `job` comes from the copied `tags_to_columns`,';
SELECT '-- and there is no `min_time`/`max_time` because the copied `store_min_time_and_max_time` is 0';
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries;
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';

SELECT '-- `AS` with a `SETTINGS` clause: the written `store_min_time_and_max_time` overrides the copied one so';
SELECT '-- `min_time`/`max_time` appear, the written `aggregate_min_time_and_max_time` is added so they are not';
SELECT '-- aggregated, and `tags_to_columns` is still copied from `ts_src`';
CREATE TABLE ts_derived AS ts_src ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 1, aggregate_min_time_and_max_time = 0;
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_derived';

-- The `job` column comes with the copied inner columns anyway, so check that it's actually filled -
-- that needs the `tags_to_columns` setting. The database is passed explicitly because with parallel
-- replicas the query can go to a replica where the current database is different.
SELECT '-- the copied `tags_to_columns` fills the dedicated column';
INSERT INTO ts_derived (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(1, 1.)]);
SELECT metric_name, job FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_derived') ORDER BY metric_name;
DROP TABLE ts_derived;

SELECT '-- `AS` with a reset: the copied `store_min_time_and_max_time = 0` is not inherited, so the setting';
SELECT '-- gets its default value and `min_time`/`max_time` appear, while `tags_to_columns` is still copied';
CREATE TABLE ts_derived AS ts_src ENGINE = TimeSeries
SETTINGS aggregate_min_time_and_max_time = 0, store_min_time_and_max_time = DEFAULT;
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_derived';
DROP TABLE ts_derived;

SELECT '-- a written value wins over a reset of the same setting, so there is no `min_time`/`max_time`';
CREATE TABLE ts_derived AS ts_src ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, store_min_time_and_max_time = DEFAULT;
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_derived';

DROP TABLE ts_derived;
DROP TABLE ts_copy;
DROP TABLE ts_src;
