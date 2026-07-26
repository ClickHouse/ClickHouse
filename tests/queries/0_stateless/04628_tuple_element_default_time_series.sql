-- Tags: no-replicated-database
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables synchronously.

-- Regression test: `DEFAULT` expressions inside `Tuple` data types must also be normalized for the
-- columns of the inner tables of a `TimeSeries` table, which are reified before the usual
-- `CREATE TABLE` normalization. See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_tuple_default;

CREATE TABLE ts_tuple_default ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    timestamp DateTime64(3),
    value Float64,
    extra Tuple(a UInt8, s String DEFAULT 'Hello')
);

SELECT type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' AND name = 'extra';

DROP TABLE ts_tuple_default;

-- The same for the outer columns of a `TimeSeries` table.
DROP TABLE IF EXISTS ts_tuple_default_outer;

CREATE TABLE ts_tuple_default_outer (extra Tuple(a UInt8, s String DEFAULT 'World')) ENGINE = TimeSeries;

SELECT type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'ts_tuple_default_outer' AND name = 'extra';

DROP TABLE ts_tuple_default_outer;
