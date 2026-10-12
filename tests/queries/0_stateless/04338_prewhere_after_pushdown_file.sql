-- Tags: no-fasttest
-- no-fasttest: uses Parquet format which is not available in fast test.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106833
-- A query over a file()/Parquet source with both an explicit PREWHERE and a WHERE used to
-- crash with "Logical error: updateFormatPrewhereInfo called more than once" when
-- optimize_prewhere_after_pushdown = 1: the second PREWHERE promotion pass invoked
-- updatePrewhereInfo() a second time on a format reader that can only accept one update.

SET enable_analyzer = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('04338_prewhere_after_pushdown_' || currentDatabase() || '.parquet', Parquet)
    SELECT number AS x, number % 2 AS y FROM numbers(10);

-- The crashing query from the issue: must run, not abort the server.
SELECT count() FROM file('04338_prewhere_after_pushdown_' || currentDatabase() || '.parquet', Parquet)
PREWHERE x > 2 WHERE y = 1
SETTINGS optimize_prewhere_after_pushdown = 1;

-- The result must match the same query with the second pass disabled.
SELECT count() FROM file('04338_prewhere_after_pushdown_' || currentDatabase() || '.parquet', Parquet)
PREWHERE x > 2 WHERE y = 1
SETTINGS optimize_prewhere_after_pushdown = 0;

-- Returned rows must also be identical regardless of the setting.
SELECT x FROM file('04338_prewhere_after_pushdown_' || currentDatabase() || '.parquet', Parquet)
PREWHERE x > 2 WHERE y = 1
ORDER BY x
SETTINGS optimize_prewhere_after_pushdown = 1;
