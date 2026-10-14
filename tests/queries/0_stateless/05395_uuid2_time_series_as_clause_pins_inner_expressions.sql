-- Tags: no-ordinary-database, no-replicated-database
-- The test finds the inner tables of a TimeSeries table by the Atomic inner-table names (`.inner_id.tags.<uuid>`).

-- `CREATE TABLE ... AS <time_series>` under `uuid_type_version = 2` keeps the historical `UUID` semantics of the inner
-- expressions copied from the old table (such as a `CAST(..., 'UUID')` default), while a bare `UUID` written in the
-- query itself is materialized to `UUID2`.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET allow_experimental_time_series_table = 1;

SET uuid_type_version = 1;
CREATE TABLE ts_old ENGINE = TimeSeries
    TAGS INNER COLUMNS (id UUID DEFAULT CAST(reinterpretAsUUID(sipHash128(metric_name, tags)), 'UUID'));

SET uuid_type_version = 2;
CREATE TABLE ts_copy AS ts_old;
CREATE TABLE ts_new ENGINE = TimeSeries
    TAGS INNER COLUMNS (id UUID DEFAULT CAST(reinterpretAsUUID(sipHash128(metric_name, tags)), 'UUID'));

SELECT t.name, c.type, c.default_expression
FROM system.columns AS c
INNER JOIN system.tables AS t ON c.database = t.database AND c.table = '.inner_id.tags.' || toString(t.uuid)
WHERE c.database = currentDatabase() AND t.database = currentDatabase() AND c.name = 'id'
ORDER BY t.name;

-- The copied table works: the default produces ids of the same type as the column.
INSERT INTO ts_copy (metric_name, tags, samples) VALUES ('http_requests', map('job', 'api'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.0)]);
SELECT metric_name, tags FROM ts_copy;

DROP TABLE ts_old;
DROP TABLE ts_copy;
DROP TABLE ts_new;
