-- `CREATE TABLE ... AS <TimeSeries table>` copies the inner engines of the other table, but the metric families
-- table names the column with the name of a metric family differently in versions before 6, so the copied keys
-- referring to it must be dropped and generated again for the version of the new table.

DROP TABLE IF EXISTS ts_mf_keys_src;
DROP TABLE IF EXISTS ts_mf_keys_copy;
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_mf_keys_src;
DROP TABLE IF EXISTS ts_mf_keys_copy;

CREATE TABLE ts_mf_keys_src ENGINE = TimeSeries SETTINGS version = 5
METRIC FAMILIES INNER ENGINE = MergeTree PRIMARY KEY metric_family_name ORDER BY (metric_family_name, type);

CREATE TABLE ts_mf_keys_copy AS ts_mf_keys_src ENGINE = TimeSeries;

SELECT engine, primary_key, sorting_key FROM system.tables
WHERE database = currentDatabase()
  AND name = '.inner_id.metricfamilies.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_mf_keys_copy');

INSERT INTO ts_mf_keys_copy (metric_name, tags, samples, metric_family, type, unit, help)
VALUES ('http_requests_total', {'job': 'test'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests');

SELECT metric_family, type, unit, help FROM ts_mf_keys_copy ORDER BY metric_family;

DROP TABLE ts_mf_keys_copy;
DROP TABLE ts_mf_keys_src;
