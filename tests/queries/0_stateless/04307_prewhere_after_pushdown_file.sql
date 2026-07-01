-- Tags: no-fasttest
-- Logical error 'updateFormatPrewhereInfo called more than once' for file() with an explicit PREWHERE
-- plus a WHERE under optimize_prewhere_after_pushdown.
-- https://github.com/ClickHouse/ClickHouse/issues/106833

INSERT INTO FUNCTION file('04307_data_' || currentDatabase() || '.parquet', Parquet, 'x UInt8, y UInt8, z UInt32')
SELECT number % 2, intDiv(number, 2) % 2, number FROM numbers(8)
SETTINGS engine_file_truncate_on_insert = 1;

-- Reported repro: must not throw and must return the same rows whether the second promotion runs or not.
SELECT z FROM file('04307_data_' || currentDatabase() || '.parquet', Parquet, 'x UInt8, y UInt8, z UInt32')
PREWHERE x WHERE y ORDER BY z SETTINGS optimize_prewhere_after_pushdown = 1;

SELECT z FROM file('04307_data_' || currentDatabase() || '.parquet', Parquet, 'x UInt8, y UInt8, z UInt32')
PREWHERE x WHERE y ORDER BY z SETTINGS optimize_prewhere_after_pushdown = 0;

-- A WHERE with several conjuncts promoted on top of the explicit PREWHERE.
SELECT z FROM file('04307_data_' || currentDatabase() || '.parquet', Parquet, 'x UInt8, y UInt8, z UInt32')
PREWHERE x WHERE y AND z > 3 ORDER BY z SETTINGS optimize_prewhere_after_pushdown = 1;
