DROP TABLE IF EXISTS collapsing_table;
SET optimize_on_insert = 0;

CREATE TABLE collapsing_table
(
    key UInt64,
    value UInt64,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY key
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate=0,
    vertical_merge_algorithm_min_columns_to_activate=0,
    min_bytes_for_wide_part = 0;

INSERT INTO collapsing_table SELECT if(number == 8192, 8191, number), 1, if(number == 8192, +1, -1) FROM numbers(8193);

SELECT sum(Sign), count() from collapsing_table;

OPTIMIZE TABLE collapsing_table FINAL;

SELECT sum(Sign), count() from collapsing_table;

DROP TABLE IF EXISTS collapsing_table;


DROP TABLE IF EXISTS collapsing_suspicious_granularity;

CREATE TABLE collapsing_suspicious_granularity
(
    key UInt64,
    value UInt64,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY key
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate=0,
    vertical_merge_algorithm_min_columns_to_activate=0,
    min_bytes_for_wide_part = 0,
    index_granularity = 1;

INSERT INTO collapsing_suspicious_granularity VALUES (1, 1, -1) (1, 1, 1);

SELECT sum(Sign), count() from collapsing_suspicious_granularity;

OPTIMIZE TABLE collapsing_suspicious_granularity FINAL;

SELECT sum(Sign), count() from collapsing_suspicious_granularity;


DROP TABLE IF EXISTS collapsing_suspicious_granularity;
