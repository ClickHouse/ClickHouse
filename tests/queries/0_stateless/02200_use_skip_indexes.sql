CREATE TABLE data_02200 (
    key Int,
    value Int,
    INDEX idx value TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key
PARTITION BY key;

INSERT INTO data_02200 SELECT number, number FROM numbers(10);

-- { echoOn }
SELECT * FROM data_02200 WHERE value = 1 SETTINGS use_skip_indexes=1, max_rows_to_read=1;
SELECT * FROM data_02200 WHERE value = 1 SETTINGS use_skip_indexes=0, max_rows_to_read=1; -- { serverError TOO_MANY_ROWS }
