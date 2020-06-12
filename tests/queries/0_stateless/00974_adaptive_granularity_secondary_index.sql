
DROP TABLE IF EXISTS indexed_table;

CREATE TABLE indexed_table
(
    `tm` DateTime,
    `log_message` String,
    INDEX log_message log_message TYPE tokenbf_v1(4096, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tm)
SETTINGS index_granularity_bytes = 50;

INSERT INTO indexed_table SELECT toDateTime('2019-05-27 10:00:00') + number % 100, 'h' FROM numbers(1000);

INSERT INTO indexed_table
SELECT
    toDateTime('2019-05-27 10:00:00') + number % 100,
    concat('hhhhhhhhhhhhhhhhhhhhhhhhh', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'yyyyyyyyyyyyyyyyyyyyyyyyyy', toString(rand()))
FROM numbers(1000);

OPTIMIZE TABLE indexed_table FINAL;

SELECT COUNT() FROM indexed_table WHERE log_message like '%x%';

DROP TABLE IF EXISTS indexed_table;

DROP TABLE IF EXISTS another_indexed_table;

CREATE TABLE another_indexed_table
(
  `tm` DateTime,
  `log_message` String,
  INDEX log_message log_message TYPE tokenbf_v1(4096, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tm)
SETTINGS index_granularity_bytes = 50,
         vertical_merge_algorithm_min_rows_to_activate=0,
         vertical_merge_algorithm_min_columns_to_activate=0;


INSERT INTO another_indexed_table SELECT toDateTime('2019-05-27 10:00:00') + number % 100, 'h' FROM numbers(1000);

INSERT INTO another_indexed_table
SELECT
  toDateTime('2019-05-27 10:00:00') + number % 100,
  concat('hhhhhhhhhhhhhhhhhhhhhhhhh', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'yyyyyyyyyyyyyyyyyyyyyyyyyy', toString(rand()))
  FROM numbers(1000);

OPTIMIZE TABLE another_indexed_table FINAL;

SELECT COUNT() FROM another_indexed_table WHERE log_message like '%x%';

DROP TABLE IF EXISTS another_indexed_table;
