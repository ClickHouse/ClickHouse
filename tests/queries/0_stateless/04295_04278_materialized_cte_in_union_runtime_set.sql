SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04278 SYNC;
CREATE TABLE t_04278 (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04278 VALUES (1, 10), (2, 20), (3, 30);

-- Query A — minimal regression shape.
--
-- Materialized CTE referenced from a UNION ALL inside an IN-subquery,
-- where the IN column is NOT the primary key, so neither IN can use inplace
-- materialization via PK analysis. Before the fix,
-- `removeTopLevelDelayedMaterializingCTEsStep` stripped only the union-level
-- safety-net `DelayedMaterializingCTEsStep` planted by `buildPlanForUnionNode`
-- in the runtime set sub-plan, leaving the per-branch safety-nets in place.
-- The sub-plan's recursive `plan->optimize` then ran `resolveMaterializingCTEs`
-- pre-order: branch 1's per-branch step won `is_materialization_planned`, the
-- writer was attached as a sibling inside branch 1, branch 2 became a
-- degenerate `MaterializingCTEs`, and the outer plan's later
-- `resolveMaterializingCTEs` had nothing left to claim and also degenerated.
-- Branch 2's reader fired before branch 1's writer finished, tripping the
-- `is_built` fail-fast at `ReadFromMemoryStorageStep.cpp:86`.
WITH ct AS MATERIALIZED (SELECT b FROM t_04278)
SELECT count()
FROM t_04278
WHERE b IN (
    SELECT b FROM ct WHERE b = 10
    UNION ALL
    SELECT b FROM ct WHERE b = 20
);

DROP TABLE t_04278;

-- Query B — verbatim AST-fuzzer query that originally surfaced the failure
-- (https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=105041&sha=latest&name_0=PR).
-- ReplacingMergeTree because the fuzzer's FINAL keyword requires it; ORDER BY a
-- so `b` stays non-PK (matching the fuzzer's `t2_04227__fuzz_37` schema). The
-- shape exercises:
--   * GLOBAL IN whose subquery is a UNION DISTINCT of two `ct` references
--     (the per-branch safety-net race);
--   * a regular `WHERE b IN (SELECT b FROM ct)` (the simpler runtime-set path);
--   * non-materialized `rs` CTE inlined into the outer query (so all `ct`
--     references end up in the outer plan, not in a nested materialized CTE);
--   * PREWHERE / FINAL / QUALIFY / constant filters that the fuzzer injected.
-- Wrapped in `SELECT count() FROM (...)` so the test pins a single integer
-- instead of an unordered row set.
DROP TABLE IF EXISTS t2_04227__fuzz_37 SYNC;
CREATE TABLE t2_04227__fuzz_37 (a Int32, b Int32) ENGINE = ReplacingMergeTree ORDER BY a;
INSERT INTO t2_04227__fuzz_37 VALUES (1, 10), (2, 20), (3, 30);

SELECT count() FROM (
    WITH ct AS MATERIALIZED (SELECT DISTINCT b FROM t2_04227__fuzz_37 FINAL PREWHERE 1023 LIMIT 2147483646),
         rs AS (SELECT b GLOBAL IN (SELECT DISTINCT b FROM ct WHERE 17 LIMIT 69 UNION DISTINCT SELECT b FROM ct LIMIT 1), *
                FROM t2_04227__fuzz_37 FINAL
                PREWHERE toLowCardinality(1048575)
                WHERE b IN (SELECT b FROM ct)
                QUALIFY 65536)
    SELECT * FROM rs AS x WHERE 65535
);

DROP TABLE t2_04227__fuzz_37;
