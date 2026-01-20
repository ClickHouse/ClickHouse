DROP TABLE IF EXISTS test_statistic_negative_max;

CREATE TABLE test_statistic_negative_max (
    id Int32,
    value Int8
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS auto_statistics_types = 'minmax';

-- Insert negative Int8 values
INSERT INTO test_statistic_negative_max VALUES (1, -100), (2, -50), (3, -1);

SELECT
    column,
    estimates.min,
    estimates.max
FROM system.parts_columns
WHERE (table = 'test_statistic_negative_max') AND (column = 'value');

DROP TABLE IF EXISTS test_statistic_negative_max;
