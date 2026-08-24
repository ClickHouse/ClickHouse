-- https://github.com/ClickHouse/ClickHouse/issues/7520
SET enable_analyzer=1;
CREATE TABLE test (`a` UInt32, `b` UInt32) ENGINE = Memory;

INSERT INTO test VALUES (1,2), (1,3), (2,4);

-- 1	5
-- 2	4

WITH
    a as key
SELECT
    a as k1,
    sum(b) as k2
FROM
    test
GROUP BY
    key
ORDER BY k1, k2;

WITH a as key SELECT key as k1 FROM test GROUP BY key ORDER BY key;

WITH a as key SELECT key as k1 FROM test ORDER BY key;
