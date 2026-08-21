-- Tags: no-fasttest, no-replicated-database
-- `timeSeriesSelector` needs ANTLR4, which is disabled in the fast-test build.
-- `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously.

-- Regression test for whole-metric primary-key ranges with the default `UUID2` component.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET allow_experimental_time_series_table = 1;
SET uuid_type_version = 2;

DROP TABLE IF EXISTS ts_uuid2_range;
CREATE TABLE ts_uuid2_range ENGINE = TimeSeries;

INSERT INTO ts_uuid2_range (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(200, 3), 2.)]);

SELECT toTypeName(id) FROM timeSeriesSelector(ts_uuid2_range, 'foo', 0, 1000) LIMIT 1;

-- The whole-metric lookup must produce a primary-key range covering every `id` of the metric: from the
-- all-zero `UUID2` up to the all-`f` one. The bounds are matched by value rather than by a `toUUID2` call,
-- because the plan renders the constant as a folded tuple literal.
SELECT plan LIKE '%00000000-0000-0000-0000-000000000000%'
   AND plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_uuid2_id_range
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
    FROM
    (
        EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector(ts_uuid2_range, 'foo', 0, 1000)
    )
);

DROP TABLE ts_uuid2_range;
