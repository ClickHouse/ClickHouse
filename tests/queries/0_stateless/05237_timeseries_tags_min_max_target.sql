-- Tags: no-fasttest
-- Tag no-fasttest: the selector below parses PromQL, which needs ANTLR4, disabled in the fast-test build.
--
-- Version 6 moves `min_time` / `max_time` out of the tags target into a separate `TAGS MIN MAX`
-- target. The tags target then holds only identity columns, so it no longer needs an aggregating
-- engine nor `allow_dimensions_outside_sorting_key`. A table pinned to version 5 keeps the old
-- single-table layout forever. The exact generated DDL is asserted in
-- gtest_normalize_time_series_definition; this test checks the tables a server really creates and
-- that the bounds are written and read back through the new target.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_v6;
DROP TABLE IF EXISTS ts_v5;
DROP TABLE IF EXISTS ts_v5_copy;
DROP TABLE IF EXISTS ts_no_bounds;

SELECT '-- a new table is split, and its tags target loses the engine that carried the bounds';

CREATE TABLE ts_v6 ENGINE = TimeSeries;

SELECT extract(create_table_query, 'version = (\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v6';

SELECT countIf(name IN ('min_time', 'max_time')) FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner\_id.tags.%';

SELECT engine, engine_full LIKE '%allow_dimensions_outside_sorting_key%' AS keeps_dimension_setting
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.tags.%';

SELECT '-- the tags min max target carries them, keyed the same way';

SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner\_id.tagsminmax.%' ORDER BY position;

SELECT engine, engine_full LIKE '%PRIMARY KEY metric_name ORDER BY (metric_name, id)%' AS keyed_like_tags
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.tagsminmax.%';

SELECT '-- an insert fills both targets and the bounds follow the samples';

INSERT INTO ts_v6 (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64(1000, 3), 1.0), (toDateTime64(1060, 3), 2.0)]),
    ('http_requests', {'job': 'web'}, [(toDateTime64(5000, 3), 3.0)]);

SELECT count() FROM timeSeriesTags(currentDatabase(), 'ts_v6');
SELECT count() FROM timeSeriesTagsMinMax(currentDatabase(), 'ts_v6');
SELECT min_time, max_time FROM timeSeriesTagsMinMax(currentDatabase(), 'ts_v6') ORDER BY min_time;

SELECT '-- a range query still returns exactly the series it must';

SELECT count() FROM timeSeriesSelector(ts_v6, 'http_requests', toDateTime64(900, 3), toDateTime64(1100, 3));
SELECT count() FROM timeSeriesSelector(ts_v6, 'http_requests', toDateTime64(0, 3), toDateTime64(500, 3));
SELECT count() FROM timeSeriesSelector(ts_v6, 'http_requests', toDateTime64(0, 3), toDateTime64(9000, 3));

SELECT '-- a series whose bounds row is missing must not vanish from a read';

-- Written straight into the tags target, so the sink never gave it a bounds row. A read that joined
-- the two targets the wrong way round would drop the series entirely.
INSERT INTO FUNCTION timeSeriesTags(currentDatabase(), 'ts_v6') (metric_name, tags)
    VALUES ('orphan_series', {'job': 'api'});
INSERT INTO FUNCTION timeSeriesSamples(currentDatabase(), 'ts_v6') (id, timestamp, value)
    SELECT id, toDateTime64(1000, 3), 9.0 FROM timeSeriesTags(currentDatabase(), 'ts_v6')
    WHERE metric_name = 'orphan_series';

SELECT count() FROM timeSeriesSelector(ts_v6, 'orphan_series', toDateTime64(0, 3), toDateTime64(9000, 3));

DROP TABLE ts_v6;

SELECT '-- a table pinned to version 5 keeps the old single-table layout';

CREATE TABLE ts_v5 ENGINE = TimeSeries SETTINGS version = 5;

SELECT position(create_table_query, 'TAGS MIN MAX') > 0 AS has_tags_min_max_clause
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v5';

SELECT countIf(name IN ('min_time', 'max_time')) FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner\_id.tags.%';

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\_id.tagsminmax.%';

SELECT '-- `AS` a version 5 table produces a version 6 split copy';

CREATE TABLE ts_v5_copy AS ts_v5;

SELECT extract(create_table_query, 'version = (\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v5_copy';

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = concat('.inner_id.tagsminmax.',
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v5_copy'));

DROP TABLE ts_v5_copy;
DROP TABLE ts_v5;

SELECT '-- without stored bounds there is no tags min max target at all';

CREATE TABLE ts_no_bounds ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0;

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\_id.tagsminmax.%';

SELECT position(create_table_query, 'TAGS MIN MAX') > 0 AS has_tags_min_max_clause
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_no_bounds';

DROP TABLE ts_no_bounds;
