-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the replicas execute the `timeSeriesMetricFamilies` table function without coordinating
-- with the initiator, so a `count()` answered from the primary key index alone is summed up once per replica,
-- see https://github.com/ClickHouse/ClickHouse/issues/118130.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts, ts2, ts3, ts_v6, ts_tags, ext_metric_families;

-- The inner tables use MergeTree, so the counts below don't depend on background merges.
CREATE TABLE ts ENGINE = TimeSeries TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id) METRIC FAMILIES INNER ENGINE = MergeTree ORDER BY metric_family;
CREATE TABLE ext_metric_families (metric_family String, type LowCardinality(String), unit LowCardinality(String), help String, CONSTRAINT c CHECK metric_family != 'bad') ENGINE = MergeTree ORDER BY metric_family;

SELECT '--- the same metric family is written once, a changed description is a new row, rows without a metric family are skipped ---';
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first'), ('m', 'counter', 'bytes', 'second'), ('', '', '', ''), ('m2', 'gauge', '', 'third');
SELECT metric_family, count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) GROUP BY metric_family ORDER BY metric_family;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('', 'gauge', '', ''); -- { serverError INCORRECT_DATA }

SELECT '--- a description written before is written again after a different one, so the table gets the latest ---';
INSERT INTO ts (metric_family, type, unit, help) VALUES ('f1', 'gauge', '', 'A');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('f1', 'gauge', '', 'B');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('f1', 'gauge', '', 'A');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('f2', 'gauge', '', 'A'), ('f2', 'gauge', '', 'B'), ('f2', 'gauge', '', 'A');
SELECT metric_family, count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family LIKE 'f%' GROUP BY metric_family ORDER BY metric_family;

SELECT '--- one insert writes a repeated metric family once, within the cache capacity ---';
INSERT INTO ts (metric_family, type, unit, help) VALUES ('r1', 'gauge', '', ''), ('r1', 'gauge', '', ''), ('r2', 'gauge', '', ''), ('r1', 'gauge', '', '');
INSERT INTO ts (metric_family, type, unit, help) SELECT 'r3', 'gauge', '', '' FROM numbers(10) SETTINGS max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SELECT metric_family, count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family LIKE 'r%' GROUP BY metric_family ORDER BY metric_family;

SELECT '--- SYSTEM CLEAR TIME SERIES CACHES makes the next insert write its rows again ---';
SYSTEM CLEAR TIME SERIES CACHES ts;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family = 'm';
SYSTEM CLEAR TIME SERIES CACHES ext_metric_families; -- { serverError UNEXPECTED_TABLE_ENGINE }
SYSTEM CLEAR TIME SERIES CACHES unknown_table; -- { serverError UNKNOWN_TABLE }

SELECT '--- TRUNCATE TABLE clears the caches ---';
TRUNCATE TABLE ts;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts);

SELECT '--- changing the limits of a cache keeps its entries ---';
ALTER TABLE ts MODIFY SETTING metric_families_deduplication_cache_expiration_seconds = 7200;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts);

SELECT '--- the least recently used entry is evicted when the cache is full, a used entry survives ---';
SYSTEM CLEAR TIME SERIES CACHES ts;
ALTER TABLE ts MODIFY SETTING metric_families_deduplication_cache_size_bytes = 456; -- three entries of about 152 bytes, see APPROXIMATE_ENTRY_SIZE in TimeSeriesDeduplicationCache.h
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e1', 'gauge', '', '');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e2', 'gauge', '', '');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e3', 'gauge', '', '');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e1', 'gauge', '', ''); -- a hit, e2 is the least recently used entry now
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e4', 'gauge', '', ''); -- evicts e2
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e2', 'gauge', '', ''); -- written again
INSERT INTO ts (metric_family, type, unit, help) VALUES ('e1', 'gauge', '', ''); -- still a hit
SELECT metric_family, count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family LIKE 'e%' GROUP BY metric_family ORDER BY metric_family;

SELECT '--- an entry expires after the expiration period ---';
ALTER TABLE ts MODIFY SETTING metric_families_deduplication_cache_size_bytes = 10000, metric_families_deduplication_cache_expiration_seconds = 1;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m4', 'gauge', '', '');
SELECT sleep(1.5) FORMAT Null;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m4', 'gauge', '', '');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family = 'm4';

SELECT '--- the cache is disabled by setting its size to 0 ---';
ALTER TABLE ts MODIFY SETTING metric_families_deduplication_cache_size_bytes = 0;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m5', 'gauge', '', '');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m5', 'gauge', '', '');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family = 'm5';

SELECT '--- the cache is enabled again by resetting its size, it starts empty ---';
ALTER TABLE ts RESET SETTING metric_families_deduplication_cache_size_bytes, metric_families_deduplication_cache_expiration_seconds;
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m5', 'gauge', '', '');
INSERT INTO ts (metric_family, type, unit, help) VALUES ('m5', 'gauge', '', '');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_family = 'm5';

SELECT '--- the tags table is not deduplicated if min_time and max_time are stored ---';
INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts);
ALTER TABLE ts MODIFY SETTING tags_deduplication_cache_size_bytes = 10; -- { serverError INVALID_SETTING_VALUE }

SELECT '--- the same time series is written to the tags table once if min_time and max_time are not stored ---';
CREATE TABLE ts_tags ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0 TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_tags (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO ts_tags (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]), ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_tags);
SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_tags);
SYSTEM CLEAR TIME SERIES CACHES ts_tags;
INSERT INTO ts_tags (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:03', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_tags);

SELECT '--- tables of earlier versions have no caches and cannot set them ---';
CREATE TABLE ts_v6 ENGINE = TimeSeries SETTINGS version = 6, metric_families_deduplication_cache_size_bytes = 10; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_v6 ENGINE = TimeSeries SETTINGS version = 6 METRIC FAMILIES INNER ENGINE = MergeTree ORDER BY metric_family;
INSERT INTO ts_v6 (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts_v6 (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE:Identifier}.ts_v6);
ALTER TABLE ts_v6 MODIFY SETTING metric_families_deduplication_cache_expiration_seconds = 10; -- { serverError INVALID_SETTING_VALUE }

SELECT '--- two tables have separate caches for a shared external metric families table ---';
CREATE TABLE ts2 ENGINE = TimeSeries METRIC FAMILIES ext_metric_families;
CREATE TABLE ts3 ENGINE = TimeSeries METRIC FAMILIES ext_metric_families;
INSERT INTO ts2 (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts2 (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts3 (metric_family, type, unit, help) VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM ext_metric_families;

SELECT '--- a failed insert does not fill the cache ---';
INSERT INTO ts2 (metric_family, type, unit, help) VALUES ('bad', 'gauge', '', ''); -- { serverError VIOLATED_CONSTRAINT }
ALTER TABLE ext_metric_families DROP CONSTRAINT c;
INSERT INTO ts2 (metric_family, type, unit, help) VALUES ('bad', 'gauge', '', '');
SELECT count() FROM ext_metric_families WHERE metric_family = 'bad';

DROP TABLE ts, ts2, ts3, ts_v6, ts_tags, ext_metric_families;
