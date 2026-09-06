-- Tags: no-replicated-database
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables synchronously.

-- Regression test: `DEFAULT` expressions inside `Tuple` data types must also be normalized for the columns
-- of the inner tables of a `TimeSeries` table. `normalizeTimeSeriesDefinition` reifies the declared types
-- of `SAMPLES INNER COLUMNS` before the usual `CREATE TABLE` normalization, so without pulling the defaults
-- up first the declaration was rejected with `BAD_ARGUMENTS`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_tuple_default;

CREATE TABLE ts_tuple_default ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    timestamp DateTime64(3),
    value Float64,
    extra Tuple(a UInt8, s String DEFAULT 'Hello')
)
TAGS INNER COLUMNS
(
    extra Tuple(b Int64 DEFAULT -1)
);

SELECT splitByChar('.', table)[3] AS kind, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.%' AND name = 'extra'
ORDER BY kind;

DROP TABLE ts_tuple_default;
