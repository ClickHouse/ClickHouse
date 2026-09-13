-- Tags: no-replicated-database, no-parallel-replicas
-- Tag no-replicated-database: plain `DETACH TABLE` is not allowed there, only `DETACH TABLE PERMANENTLY`.
-- Tag no-parallel-replicas: the initiator sends the argument of `timeSeriesTags` to the replicas unqualified,
-- so they look the table up in the `default` database, see https://github.com/ClickHouse/ClickHouse/issues/118130.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job'}, store_min_time_and_max_time = 0,
         filter_by_min_time_and_max_time = 0, samples_index_granularity = 1024;

INSERT INTO ts (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(1, 1.)]);

-- The tests below print the names of the settings kept in the outer `SETTINGS` clause of the create
-- query. That clause is the first line starting with `SETTINGS` of the formatted create query - the rest
-- of the query describes the inner target tables and depends on the build configuration. The values are
-- left out, they are checked through the behaviour of the table instead.
SELECT '-- the `SETTINGS` clause after `CREATE`';
SELECT arraySort(extractAll(arrayFirst(line -> line LIKE 'SETTINGS %', splitByChar('\n', formatQuery(create_table_query))), '([a-z_]+) = '))
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

SELECT '-- `MODIFY SETTING` adds the altered setting and keeps the other ones';
ALTER TABLE ts MODIFY SETTING id_generator = 'tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags))))';
SELECT arraySort(extractAll(arrayFirst(line -> line LIKE 'SETTINGS %', splitByChar('\n', formatQuery(create_table_query))), '([a-z_]+) = '))
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

SELECT '-- `tags_to_columns` survives the alter, so the dedicated column is still filled';
INSERT INTO ts (metric_name, tags, time_series) VALUES ('m2', {'job': 'j2'}, [(2, 2.)]);
SELECT metric_name, job FROM timeSeriesTags(ts) ORDER BY metric_name;

SELECT '-- the settings are kept in the metadata too, so they still apply after a reload';
DETACH TABLE ts;
ATTACH TABLE ts;
SELECT arraySort(extractAll(arrayFirst(line -> line LIKE 'SETTINGS %', splitByChar('\n', formatQuery(create_table_query))), '([a-z_]+) = '))
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
INSERT INTO ts (metric_name, tags, time_series) VALUES ('m3', {'job': 'j3'}, [(3, 3.)]);
SELECT metric_name, job FROM timeSeriesTags(ts) ORDER BY metric_name;

SELECT '-- `MODIFY SETTING` sees the other settings, so a conflicting value is rejected';
ALTER TABLE ts MODIFY SETTING filter_by_min_time_and_max_time = 1; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- `RESET SETTING` removes only the reset setting';
ALTER TABLE ts RESET SETTING filter_by_min_time_and_max_time;
SELECT arraySort(extractAll(arrayFirst(line -> line LIKE 'SETTINGS %', splitByChar('\n', formatQuery(create_table_query))), '([a-z_]+) = '))
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

DROP TABLE ts;
