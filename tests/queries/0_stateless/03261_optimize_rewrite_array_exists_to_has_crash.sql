-- https://github.com/ClickHouse/ClickHouse/issues/71382
DROP TABLE IF EXISTS rewrite;
CREATE TABLE rewrite (c0 Int) ENGINE = Memory();
SELECT 1
FROM rewrite
INNER JOIN rewrite AS y ON (
    SELECT 1
)
INNER JOIN rewrite AS z ON 1
SETTINGS optimize_rewrite_array_exists_to_has=1;
