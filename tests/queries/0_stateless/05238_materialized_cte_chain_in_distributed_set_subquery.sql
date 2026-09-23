-- A distributed read whose uncorrelated IN set subquery declares a chain of MATERIALIZED CTEs lost the
-- materialization gate for the inner CTE of the chain, so a reader ran before its writer: "Reading from
-- materialized CTE 'c' before its materialization completed - DelayedPortsProcessor gate is missing in
-- the query plan".
-- Random settings limits: prefer_localhost_replica=(1, None)

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;
-- Pin: the local shard must be served by a shard-local plan, which is the tree where the CTE references
-- arrive as bare temporary table names with no body. With 0 there is no such tree and nothing is tested.
SET prefer_localhost_replica = 1;
-- Pin: selects the in-place set build, the route that claims the CTEs during index analysis. The runtime
-- route is exercised as its own arm below.
SET use_index_for_in_with_subqueries = 1;

DROP TABLE IF EXISTS t_05238;
DROP TABLE IF EXISTS t_05238_upd;

-- Pin: `v` belongs to the sort key, or `v IN (<set subquery>)` never reaches KeyCondition, the set is
-- never built in place, and the shape below cannot fail.
CREATE TABLE t_05238 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY (id, v);
INSERT INTO t_05238 VALUES (1, 200);
INSERT INTO t_05238 VALUES (2, 1048578);
INSERT INTO t_05238 VALUES (3, 7);
INSERT INTO t_05238 VALUES (4, 0);

-- Pin: the set really is built during index analysis and prunes by `v`, so the arms below exercise the
-- claiming route rather than silently falling back to the runtime one.
SELECT countIf(explain LIKE '%v in 3-element set%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
        SELECT a.y FROM d AS a, d AS b));

-- Negative control for that pin: with the in-place build disabled there is no such condition, so the pin
-- measures the route rather than always holding.
SELECT countIf(explain LIKE '%v in 3-element set%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
        SELECT a.y FROM d AS a, d AS b)
    SETTINGS use_index_for_in_with_subqueries = 0);

-- Pin: this CTE structure really is materialized rather than inlined, so the value arms are not vacuous.
-- A CTE read only once is inlined, which is why `d` is read twice; `c` reaches two occurrences through
-- `d`'s two copies. Counted on the local read, where the writer steps stay on the printed plan.
SELECT countIf(explain LIKE '%MaterializingCTE (Materializing CTE:%') FROM (
    EXPLAIN SELECT count() FROM t_05238 WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
        SELECT a.y FROM d AS a, d AS b)
    SETTINGS use_index_for_in_with_subqueries = 0);

-- Negative control for that pin: without the MATERIALIZED keyword there are no writer steps at all.
SELECT countIf(explain LIKE '%MaterializingCTE (Materializing CTE:%') FROM (
    EXPLAIN SELECT count() FROM t_05238 WHERE v IN (
        WITH c AS (SELECT number AS x FROM numbers(3)),
             d AS (SELECT x + 200 AS y FROM c)
        SELECT a.y FROM d AS a, d AS b)
    SETTINGS use_index_for_in_with_subqueries = 0);

-- The minimal failing shape: a two-CTE chain behind a distributed read's IN set. The subquery yields
-- {200..202}, matching v = 200 only.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b);

-- The flattened shape: the shared CTE is both reached through `d` and referenced directly, so its two
-- registrations must still land on different levels instead of in one concurrent step.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b, c AS c1, c AS c2);

-- The reported shape verbatim: three CTEs, two of which read one shared CTE, the shared one declared
-- after its first reader, both readers used twice in a cross join. Only `d`'s values reach the set.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
    WITH e AS MATERIALIZED (SELECT x + 1048577 AS y FROM c),
         c AS MATERIALIZED (SELECT number AS x FROM numbers(3) LIMIT 99),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c LIMIT 173)
    SELECT d1.y FROM d AS d1, d AS d2, e AS e1, e AS e2);

-- The runtime route: with the in-place build disabled the set is built by DelayedCreatingSetsStep, which
-- strips the set plan's own gate and relies on the outer plan carrying one.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b)
SETTINGS use_index_for_in_with_subqueries = 0;

-- Negative control for the `prefer_localhost_replica` pin: with the local shard served over a connection
-- there is no shard-local re-analysis, so this shape passes before and after the fix.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b)
SETTINGS prefer_localhost_replica = 0;

-- The serialized-plan assembly path, which plants no gates and splices the step out on the way to a
-- remote node: the chain must survive it.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b)
SETTINGS serialize_query_plan = 1;

-- A scalar subquery instead of an IN set: another site that plans its subquery with the CTEs forced to
-- materialize. min(y) = 200, so one row matches.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v = (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT min(a.y) FROM d AS a, d AS b);

-- A lightweight UPDATE, whose mutation interpreter hand-rolls the same gate planting, with the chain
-- behind a distributed read in its predicate. Only v = 200 matches, so only row 1 changes.
-- `v` is deliberately not in this table's sort key, because it is the updated column. The distributed
-- read that carries the CTE chain is still the one on `t_05238`.
CREATE TABLE t_05238_upd (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_05238_upd VALUES (1, 200);
INSERT INTO t_05238_upd VALUES (2, 1048578);
INSERT INTO t_05238_upd VALUES (3, 7);
INSERT INTO t_05238_upd VALUES (4, 0);

UPDATE t_05238_upd SET v = 100 WHERE v IN (
    SELECT v FROM remote('127.0.0.1', currentDatabase(), 't_05238') WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
        SELECT a.y FROM d AS a, d AS b));

SELECT id, v FROM t_05238_upd ORDER BY id;

DROP TABLE t_05238;
DROP TABLE t_05238_upd;
