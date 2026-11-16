SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04227 SYNC;
CREATE TABLE t_04227 (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_04227 VALUES (1), (2), (3);

-- Original repro: reused materialized CTE filtered by IN(another materialized CTE).
-- Threw LOGICAL_ERROR before the fix because the outer DelayedMaterializingCTEsStep
-- for `ct` claimed ct.plan before rs.plan->optimize fired buildOrderedSetInplace.
WITH
    ct AS MATERIALIZED (SELECT c FROM t_04227 LIMIT 10),
    rs AS MATERIALIZED (SELECT * FROM t_04227 WHERE c IN (SELECT c FROM ct))
SELECT count()
FROM rs AS a
ANY LEFT JOIN rs AS b USING c;

-- Cross-join shape: 3 * 3 = 9.
WITH
    ct AS MATERIALIZED (SELECT c FROM t_04227 LIMIT 10),
    rs AS MATERIALIZED (SELECT * FROM t_04227 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a, rs AS b;

-- Non-PK column variant -- exercises VirtualColumnUtils::buildSetInplace rather
-- than KeyCondition::tryPrepareSetIndexForIn.
DROP TABLE IF EXISTS t2_04227 SYNC;
CREATE TABLE t2_04227 (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t2_04227 VALUES (1, 10), (2, 20), (3, 30);

WITH
    ct AS MATERIALIZED (SELECT b FROM t2_04227 LIMIT 10),
    rs AS MATERIALIZED (SELECT * FROM t2_04227 WHERE b IN (SELECT b FROM ct))
SELECT count()
FROM rs AS x
ANY LEFT JOIN rs AS y USING a;

-- EXPLAIN must succeed too (the bug fired during plan optimization).
-- We don't want to pin the plan text in the reference file, so just assert
-- that EXPLAIN produced at least one row (i.e. it didn't throw).
SELECT count() > 0
FROM (
    EXPLAIN actions = 0
    WITH
        ct AS MATERIALIZED (SELECT c FROM t_04227 LIMIT 10),
        rs AS MATERIALIZED (SELECT * FROM t_04227 WHERE c IN (SELECT c FROM ct))
    SELECT count()
    FROM rs AS a
    ANY LEFT JOIN rs AS b USING c
);

DROP TABLE t_04227;
DROP TABLE t2_04227;
