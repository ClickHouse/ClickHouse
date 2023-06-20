CREATE OR REPLACE TABLE test_table
(
    col1 String,
    col2 String,
    INDEX test_table_col2_idx col2 TYPE set(0) GRANULARITY 1
) ENGINE = MergeTree()
      ORDER BY col1
AS SELECT 'v1', 'v2';

SELECT * FROM test_table
WHERE 1 == 1 AND col1 == col1 OR
       0 AND col2 == NULL;

drop table if exists test_table;
