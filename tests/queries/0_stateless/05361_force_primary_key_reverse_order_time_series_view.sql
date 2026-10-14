-- `force_primary_key_reverse_order` must reverse the sorting keys of the hidden inner tables of a `TimeSeries`
-- table. The hidden `TimeSeries` table behind a materialized view with `ENGINE = TimeSeries` gets the default
-- engines of its inner tables only in its internal `CREATE`, which is not persisted in the definition of the view
-- and is regenerated on restore from a backup, so those inner tables must keep the original keys.

SET allow_experimental_time_series_table = 1;
SET force_primary_key_reverse_order = 1;

DROP TABLE IF EXISTS mv_ts;
DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW mv_ts ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;

-- Each inner table kind must be listed twice: with the reversed key for `ts` and with the original key for `mv_ts`.
SELECT engine, extract(create_table_query, 'ORDER BY (.+?) SETTINGS') AS sorting_key, count()
FROM system.tables
WHERE database = currentDatabase() AND engine LIKE '%MergeTree'
GROUP BY engine, sorting_key
ORDER BY engine, sorting_key;

DROP TABLE mv_ts;
DROP TABLE ts;
