-- https://github.com/ClickHouse/ClickHouse/issues/36189
SET enable_analyzer=1;
CREATE TABLE test
(
    `dt` Date,
    `text` String
)
ENGINE = MergeTree
ORDER BY dt;

insert into test values ('2020-01-01', 'text1'), ('2019-01-01', 'text2'), ('1900-01-01', 'text3');

WITH max(dt) AS maxDt
SELECT maxDt
FROM test;

WITH max(number) AS maxDt
SELECT maxDt
FROM numbers(10);
