-- https://github.com/ClickHouse/ClickHouse/issues/44365
SET enable_analyzer=1;
DROP TABLE IF EXISTS 03040_test;

CREATE TABLE 03040_test
(
    id           UInt64,
    val String alias 'value: '||toString(id)
) ENGINE = MergeTree
ORDER BY tuple();

SELECT val FROM 03040_test t GROUP BY val;

DROP TABLE IF EXISTS 03040_test;
