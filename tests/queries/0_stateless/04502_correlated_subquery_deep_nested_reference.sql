-- Nested EXISTS whose inner subquery references a column from a scope beyond its
-- immediate outer query (skipping an intermediate scope) is not supported yet. It
-- must fail with a clear NOT_IMPLEMENTED error, not an internal NOT_FOUND_COLUMN_IN_BLOCK.
-- See https://github.com/ClickHouse/ClickHouse/issues/95683

SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t04502_2;
DROP TABLE IF EXISTS t04502_3;
DROP TABLE IF EXISTS t04502_4;
DROP TABLE IF EXISTS t04502_5;

CREATE TABLE t04502_2 (vkey UInt32) ENGINE = Memory;
CREATE TABLE t04502_3 (vkey UInt32) ENGINE = Memory;
CREATE TABLE t04502_4 (vkey UInt32) ENGINE = Memory;
CREATE TABLE t04502_5 (vkey UInt32) ENGINE = Memory;

INSERT INTO t04502_2 VALUES (1), (2), (3);
INSERT INTO t04502_4 VALUES (2), (3), (4);
INSERT INTO t04502_3 VALUES (1), (2);
INSERT INTO t04502_5 VALUES (5);

-- Deep correlated reference: inner EXISTS references ref_1 from the top scope, skipping the t04502_3 scope.
SELECT ref_1.vkey
FROM t04502_2 AS ref_0
GLOBAL INNER JOIN t04502_4 AS ref_1 ON ref_0.vkey = ref_1.vkey
WHERE exists (
    SELECT ref_1.vkey
    FROM t04502_3 AS ref_2
    WHERE exists (SELECT 1 FROM t04502_5 AS ref_3 WHERE ref_1.vkey = 0)
); -- { serverError NOT_IMPLEMENTED }

-- Supported shapes keep working.

-- Single-level correlated EXISTS.
SELECT ref_1.vkey
FROM t04502_2 AS ref_0
GLOBAL INNER JOIN t04502_4 AS ref_1 ON ref_0.vkey = ref_1.vkey
WHERE exists (SELECT 1 FROM t04502_3 AS ref_2 WHERE ref_2.vkey = ref_1.vkey)
ORDER BY ref_1.vkey;

-- Nested EXISTS where the inner subquery correlates only on its immediate parent scope.
SELECT ref_1.vkey
FROM t04502_2 AS ref_0
GLOBAL INNER JOIN t04502_4 AS ref_1 ON ref_0.vkey = ref_1.vkey
WHERE exists (
    SELECT ref_2.vkey
    FROM t04502_3 AS ref_2
    WHERE exists (SELECT 1 FROM t04502_5 AS ref_3 WHERE ref_2.vkey = 0)
)
ORDER BY ref_1.vkey;

DROP TABLE t04502_2;
DROP TABLE t04502_3;
DROP TABLE t04502_4;
DROP TABLE t04502_5;
