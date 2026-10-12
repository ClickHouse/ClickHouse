-- `force_primary_key_reverse_order` must reverse the sorting keys of the hidden inner tables of a `TimeSeries`
-- table, including the one behind a materialized view with `ENGINE = TimeSeries`, whose inner tables get
-- their default engines only in the internal `CREATE` of the `TimeSeries` table.

SET allow_experimental_time_series_table = 1;
SET force_primary_key_reverse_order = 1;

DROP TABLE IF EXISTS mv_ts;
DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW mv_ts ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;

-- Each inner table definition must be listed twice: once for `ts` and once for `mv_ts`.
SELECT engine, extract(create_table_query, 'ORDER BY (.+?) SETTINGS') AS sorting_key, count()
FROM system.tables
WHERE database = currentDatabase() AND engine LIKE '%MergeTree'
GROUP BY engine, sorting_key
ORDER BY engine, sorting_key;

DROP TABLE mv_ts;
DROP TABLE ts;
