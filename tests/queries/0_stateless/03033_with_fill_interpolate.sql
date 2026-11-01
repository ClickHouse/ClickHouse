-- https://github.com/ClickHouse/ClickHouse/issues/55794
SET enable_analyzer=1;
DROP TABLE IF EXISTS 03033_example_table;

CREATE TABLE 03033_example_table
(
  ColumnA Int64,
  ColumnB Int64,
  ColumnC Int64
)
ENGINE = MergeTree()
ORDER BY ColumnA;

WITH
helper AS (
  SELECT
    *
  FROM
    03033_example_table
  ORDER BY
    ColumnA WITH FILL INTERPOLATE (
      ColumnB AS ColumnC,
      ColumnC AS ColumnA
    )
)
SELECT ColumnB FROM helper;

DROP TABLE IF EXISTS 03033_example_table;
