-- a subquery plan cannot see hypothetical projections, so force_optimize_projection = 1 must not make EXPLAIN WHATIF fail
-- the verdict still names the setting
DROP TABLE IF EXISTS t_whatif_force_nested;
CREATE TABLE t_whatif_force_nested (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
INSERT INTO t_whatif_force_nested SELECT number, number % 100, number FROM numbers(300);

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, enable_parallel_replicas = 0;

CREATE HYPOTHETICAL PROJECTION p_b ON t_whatif_force_nested (SELECT a, b, v ORDER BY b);

SELECT '-- scalar subquery';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE b = (SELECT max(b) FROM t_whatif_force_nested WHERE b >= 40) SETTINGS force_optimize_projection = 1)
WHERE match(line, '^(status|verdict|reason):');

SELECT '-- IN subquery';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE a = 42 AND b IN (SELECT b FROM t_whatif_force_nested WHERE b >= 40) SETTINGS force_optimize_projection = 1)
WHERE match(line, '^(status|verdict|reason):');

SELECT '-- IN subquery without PREWHERE, the set has its own plan step, and the statement does not fail';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE a = 42 AND b IN (SELECT b FROM t_whatif_force_nested WHERE b >= 40)
      SETTINGS force_optimize_projection = 1, optimize_move_to_prewhere = 0)
WHERE match(line, '^(status|verdict):');

SELECT '-- force_optimize_projection in the subquery only, the cost decides the outer read';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE a = 42 AND b IN (SELECT b FROM t_whatif_force_nested WHERE b >= 40 SETTINGS force_optimize_projection = 1))
WHERE match(line, '^(status|verdict|reason):');

SELECT '-- prefer for the query and force only in the IN subquery, the verdict names prefer';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE a = 42 AND b IN (SELECT b FROM t_whatif_force_nested WHERE b >= 40 SETTINGS force_optimize_projection = 1)
      SETTINGS prefer_optimize_projection = 1)
WHERE match(line, '^(status|verdict|reason):');

SELECT '-- forced for the session';
SET force_optimize_projection = 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE a = 42 AND b IN (SELECT b FROM t_whatif_force_nested WHERE b >= 40))
WHERE match(line, '^(status|verdict|reason):');

SELECT '-- a hypothetical skip index';
DROP HYPOTHETICAL PROJECTION p_b ON t_whatif_force_nested;
CREATE HYPOTHETICAL INDEX idx_v ON t_whatif_force_nested (v) TYPE minmax GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_whatif_force_nested
      WHERE v < 50 AND b IN (SELECT b FROM t_whatif_force_nested WHERE b >= 40))
WHERE match(line, '^(status|verdict|reason):');

DROP TABLE t_whatif_force_nested;
