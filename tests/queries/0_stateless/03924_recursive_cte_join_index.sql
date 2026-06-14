-- Tags: long
-- The `long` tag exempts the test from the 180s flaky-check per-run timeout:
-- it contains many distinct `WITH RECURSIVE` queries, each exercising a different
-- planner edge case, and under pathological random-settings combinations
-- (notably `max_threads = 32` with split-range injection) a single recursive
-- step can take several seconds, putting the whole test over budget even
-- though the dataset is small.

-- Test that recursive CTEs with JOINs use MergeTree primary key index.
-- Without the optimization, each recursion step scans the entire table.
-- With the optimization, join key values from the working table are injected
-- into the recursive step's WHERE clause as an `IN (...)` predicate, enabling
-- MergeTree primary key index usage.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS edges;
CREATE TABLE edges
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 8192;

-- Insert a chain: 0->1->2->...->9
INSERT INTO edges SELECT number, number + 1 FROM numbers(10);

-- Insert many unrelated rows to make index usage measurable.
-- from_id range [1000, 6000) has no connection to the chain above.
INSERT INTO edges SELECT number + 1000, number + 1000000 FROM numbers(5000);

-- Recursive CTE: traverse the chain starting from 0 using explicit JOIN.
WITH RECURSIVE traverse AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e
    INNER JOIN traverse AS t ON e.from_id = t.current_id
)
SELECT current_id FROM traverse ORDER BY current_id;

SYSTEM FLUSH LOGS query_log;

-- Check that the total rows read is small (index was used).
-- Without optimization: ~5010 * 10 steps = ~50K rows read.
-- With optimization: a few hundred rows at most.
SELECT
    read_rows < 10000 AS read_rows_ok
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE traverse AS%INNER JOIN traverse%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Also test comma-join syntax (becomes INNER JOIN after CrossToInnerJoinPass).
WITH RECURSIVE traverse2 AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e, traverse2 AS t
    WHERE e.from_id = t.current_id
)
SELECT current_id FROM traverse2 ORDER BY current_id;

SYSTEM FLUSH LOGS query_log;

SELECT
    read_rows < 10000 AS read_rows_ok
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE traverse2%FROM edges AS e, traverse2%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Self-join of the same physical table via two aliases in the recursive step.
-- Only one of the aliases joins against the CTE table; injecting the filter
-- directly into WHERE keeps it scoped to that alias, leaving the other
-- occurrence's scan unconstrained.
DROP TABLE IF EXISTS two_hop;
CREATE TABLE two_hop (from_id UInt64, to_id UInt64)
ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 8192;

INSERT INTO two_hop SELECT number, number + 1 FROM numbers(10);
INSERT INTO two_hop SELECT number + 1000, number + 1000000 FROM numbers(5000);

WITH RECURSIVE two_step AS
(
    SELECT CAST(0 AS UInt64) AS current_id
  UNION ALL
    SELECT e2.to_id AS current_id
    FROM two_hop AS e1
    INNER JOIN two_hop AS e2 ON e1.to_id = e2.from_id
    INNER JOIN two_step AS t ON e1.from_id = t.current_id
    WHERE e1.to_id < 100
)
SELECT count() FROM two_step;

-- Setting `recursive_cte_max_in_filter_cardinality = 0` disables the
-- optimization but still produces correct results.
WITH RECURSIVE traverse3 AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e
    INNER JOIN traverse3 AS t ON e.from_id = t.current_id
)
SELECT current_id FROM traverse3 ORDER BY current_id
SETTINGS recursive_cte_max_in_filter_cardinality = 0;

-- Three-branch recursive CTE where two branches reuse the same alias `x`
-- for different physical tables. Each branch's `WHERE` is independent, so
-- the injected `IN (...)` predicate is scoped to its own branch and there
-- is no cross-branch interference.
DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
CREATE TABLE t_a (col_a UInt64, val UInt64) ENGINE = MergeTree ORDER BY col_a;
CREATE TABLE t_b (col_b UInt64, val UInt64) ENGINE = MergeTree ORDER BY col_b;

INSERT INTO t_a VALUES (0, 10);
INSERT INTO t_b VALUES (10, 20);

WITH RECURSIVE rec AS
(
    SELECT CAST(0 AS UInt64) AS id
  UNION ALL
    SELECT x.val AS id FROM t_a AS x INNER JOIN rec AS r ON x.col_a = r.id
  UNION ALL
    SELECT x.val AS id FROM t_b AS x INNER JOIN rec AS r ON x.col_b = r.id
)
SELECT id FROM rec ORDER BY id;

-- Same alias `x` referring to the same physical table across two recursive
-- branches, but with different join columns. Each branch is its own
-- `QueryNode` with its own `WHERE`, so the per-branch `IN (...)` filter does
-- not over-constrain the other branch.
DROP TABLE IF EXISTS pairs;
CREATE TABLE pairs (col_a UInt64, col_b UInt64, val UInt64) ENGINE = MergeTree ORDER BY col_a;
INSERT INTO pairs VALUES (0, 100, 1) (1, 200, 2) (100, 0, 3);

WITH RECURSIVE rec_two_branch AS
(
    SELECT CAST(0 AS UInt64) AS id
  UNION ALL
    SELECT x.val AS id FROM pairs AS x INNER JOIN rec_two_branch AS r ON x.col_a = r.id
  UNION ALL
    SELECT x.val AS id FROM pairs AS x INNER JOIN rec_two_branch AS r ON x.col_b = r.id
)
SELECT id FROM rec_two_branch ORDER BY id;

-- The CTE column's type can be wider than the joined storage column's type
-- (e.g. recursive key `Int64` produces values like `-1`, joined column is
-- `UInt8`). The injected `IN (...)` filter must use the CTE column's type for
-- the RHS tuple so that values not representable in the storage column type
-- are correctly evaluated as no-match by `in` resolution, rather than throwing
-- during filter construction.
DROP TABLE IF EXISTS narrow;
CREATE TABLE narrow (id UInt8, next_id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO narrow VALUES (1, 2), (2, 3), (3, -1);

WITH RECURSIVE walk AS
(
    SELECT CAST(1 AS Int64) AS current_id
  UNION ALL
    SELECT n.next_id AS current_id
    FROM narrow AS n
    INNER JOIN walk AS w ON n.id = w.current_id
)
SELECT current_id FROM walk ORDER BY current_id;

DROP TABLE narrow;

-- A recursive branch with `HAVING` against the joined real table. The planner
-- folds non-aggregating `HAVING` into `WHERE` in place on the `QueryNode`, so
-- the recursive source has to snapshot/restore `HAVING` alongside `WHERE`.
-- Otherwise the predicate is lost on step 3+ and recursion runs past the
-- intended depth (here, past `to_id <= 2`).
DROP TABLE IF EXISTS edges_having;
CREATE TABLE edges_having (from_id UInt64, to_id UInt64)
ENGINE = MergeTree ORDER BY from_id;
INSERT INTO edges_having SELECT number, number + 1 FROM numbers(10);

WITH RECURSIVE walk_having AS
(
    SELECT CAST(0 AS UInt64) AS id
  UNION ALL
    SELECT e.to_id AS id
    FROM edges_having AS e
    INNER JOIN walk_having AS w ON e.from_id = w.id
    HAVING e.to_id <= 2
)
SELECT id FROM walk_having ORDER BY id;

DROP TABLE edges_having;

-- A restrictive `max_rows_in_set` must not change the result. The injected
-- `IN (...)` predicate is lowered into a set bounded by `max_rows_in_set` /
-- `max_bytes_in_set`; when a recursive step produces more distinct join keys
-- than that limit, injecting the filter would either throw
-- `SET_SIZE_LIMIT_EXCEEDED` (`set_overflow_mode = 'throw'`) or silently
-- truncate the set (`'break'`). Both diverge from the unoptimized scan, so the
-- optimization must fail closed and fall back to a plain scan for that step.
-- Here the tree branches (0 -> {1, 2}) so the second recursive step looks up
-- two distinct keys at once, exceeding `max_rows_in_set = 1`.
DROP TABLE IF EXISTS tree;
CREATE TABLE tree (parent UInt64, child UInt64) ENGINE = MergeTree ORDER BY parent;
INSERT INTO tree VALUES (0, 1), (0, 2), (1, 3), (2, 4);

WITH RECURSIVE walk_tree AS
(
    SELECT child AS current_id FROM tree WHERE parent = 0
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
)
SELECT current_id FROM walk_tree ORDER BY current_id
SETTINGS max_rows_in_set = 1, set_overflow_mode = 'throw';

WITH RECURSIVE walk_tree AS
(
    SELECT child AS current_id FROM tree WHERE parent = 0
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
)
SELECT current_id FROM walk_tree ORDER BY current_id
SETTINGS max_rows_in_set = 1, set_overflow_mode = 'break';

-- The same fail-closed guard must hold for recursive CTEs with more than two
-- branches, where a *single branch* carries a stricter `max_rows_in_set`. The
-- recursive part is then a synthetic `UnionNode` whose context is unlimited,
-- but the planner lowers the injected `IN` using the branch's own `QueryNode`
-- context. The set-limit preflight must therefore use the containing branch's
-- context, not the outer recursive one; otherwise the strict branch would inject
-- an oversized `IN` and throw `SET_SIZE_LIMIT_EXCEEDED` / truncate the set.
WITH RECURSIVE walk_tree AS
(
    SELECT child AS current_id FROM tree WHERE parent = 0
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
    SETTINGS max_rows_in_set = 1, set_overflow_mode = 'throw'
)
SELECT current_id FROM walk_tree ORDER BY current_id;

WITH RECURSIVE walk_tree AS
(
    SELECT child AS current_id FROM tree WHERE parent = 0
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
    SETTINGS max_rows_in_set = 1, set_overflow_mode = 'break'
)
SELECT current_id FROM walk_tree ORDER BY current_id;

DROP TABLE tree;

-- `recursive_cte_max_in_filter_cardinality` must be read from the containing
-- branch's context, not the synthetic recursive `UnionNode` context, for the
-- same reason as the set limits above. Two disjoint chains: `chain_big`
-- (0 -> ... -> 10, plus 5000 filler rows) and `chain_small`
-- (100 -> ... -> 110, no filler), each walked by its own recursive branch.
DROP TABLE IF EXISTS chain_big;
DROP TABLE IF EXISTS chain_small;
CREATE TABLE chain_big (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 8192;
CREATE TABLE chain_small (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id;
INSERT INTO chain_big SELECT number, number + 1 FROM numbers(10);
INSERT INTO chain_big SELECT number + 1000, number + 1000000 FROM numbers(5000);
INSERT INTO chain_small SELECT number + 100, number + 101 FROM numbers(10);

-- A branch-local `recursive_cte_max_in_filter_cardinality = 0` must disable
-- the optimization for that branch (the `chain_big` walk full-scans on every
-- step), while the other branch keeps its filter. Results are unaffected.
WITH RECURSIVE walk_branch_cap AS
(
    SELECT CAST(number, 'UInt64') * 100 + 1 AS id FROM numbers(2)
  UNION ALL
    SELECT e.to_id AS id
    FROM chain_big AS e
    INNER JOIN walk_branch_cap AS t ON e.from_id = t.id
    SETTINGS recursive_cte_max_in_filter_cardinality = 0
  UNION ALL
    SELECT e.to_id AS id
    FROM chain_small AS e
    INNER JOIN walk_branch_cap AS t ON e.from_id = t.id
)
SELECT id FROM walk_branch_cap ORDER BY id;

SYSTEM FLUSH LOGS query_log;

-- The disabled branch must really scan `chain_big` (5010 rows) on each of the
-- ~10 recursive steps, so the total is far above the fully-optimized count.
SELECT
    read_rows > 10000 AS branch_local_disable_respected
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE walk_branch_cap AS%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The converse: the optimization is disabled on the outer query level, but the
-- `chain_big` branch re-enables it locally. The branch's own setting must win,
-- so `chain_big` is read through the index (`chain_small`'s unoptimized walk
-- contributes only ~10 rows per step).
WITH RECURSIVE walk_branch_cap2 AS
(
    SELECT CAST(number, 'UInt64') * 100 + 1 AS id FROM numbers(2)
  UNION ALL
    SELECT e.to_id AS id
    FROM chain_big AS e
    INNER JOIN walk_branch_cap2 AS t ON e.from_id = t.id
    SETTINGS recursive_cte_max_in_filter_cardinality = 10000
  UNION ALL
    SELECT e.to_id AS id
    FROM chain_small AS e
    INNER JOIN walk_branch_cap2 AS t ON e.from_id = t.id
)
SELECT id FROM walk_branch_cap2 ORDER BY id
SETTINGS recursive_cte_max_in_filter_cardinality = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    read_rows < 10000 AS branch_local_enable_respected
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE walk_branch_cap2%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE chain_big;
DROP TABLE chain_small;

-- The set-limit preflight must measure the set the planner will actually
-- build: `CollectSets` converts the generated RHS constant to the joined
-- *storage* column's type, dropping values that are not representable in it.
-- Here the working table holds `Int64` keys `{-1, 0}`, but the storage key is
-- `UInt16`, so the planner's set contains only `{0}` — one row, within
-- `max_rows_in_set = 1`. A preflight that measured the unconverted `Int64`
-- set would see two rows and needlessly fall back to a full scan on the
-- first recursive step.
DROP TABLE IF EXISTS conv_edges;
CREATE TABLE conv_edges (parent UInt16, child UInt16) ENGINE = MergeTree ORDER BY parent SETTINGS index_granularity = 8192;
INSERT INTO conv_edges VALUES (0, 1), (1, 2);
INSERT INTO conv_edges SELECT number + 1000, number + 30000 FROM numbers(5000);

WITH RECURSIVE walk_conv AS
(
    SELECT CAST(number, 'Int64') - 1 AS id FROM numbers(2)
  UNION ALL
    SELECT toInt64(e.child) AS id
    FROM conv_edges AS e
    INNER JOIN walk_conv AS t ON e.parent = t.id
)
SELECT id FROM walk_conv ORDER BY id
SETTINGS max_rows_in_set = 1, set_overflow_mode = 'throw';

SYSTEM FLUSH LOGS query_log;

SELECT
    read_rows < 3000 AS conversion_preflight_ok
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE walk_conv%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE conv_edges;

-- The conversion the planner applies to the injected RHS can throw outright,
-- not just narrow the value set, and that must also fail closed. A recursive
-- `String` key joined against an `Enum` storage column with
-- `validate_enum_literals_in_operators = 1` is the canonical case: a frontier
-- value that is not a valid enum literal (`'z'`) makes `CollectSets` raise
-- `UNKNOWN_ELEMENT_OF_ENUM` when converting the generated `enum_col IN (...)`,
-- whereas the original `enum_col = cte_string` comparison treats it as a plain
-- no-match. The set-limit / conversion preflight runs even when the set-size
-- limits are unlimited (the default), so the optimization must skip injection
-- for that step and scan plainly, leaving the result unchanged.
DROP TABLE IF EXISTS enum_edges;
CREATE TABLE enum_edges (e Enum8('a' = 1, 'b' = 2, 'c' = 3), nxt String)
ENGINE = MergeTree ORDER BY e;
INSERT INTO enum_edges VALUES ('a', 'b'), ('b', 'z');

WITH RECURSIVE enum_walk AS
(
    SELECT CAST('a' AS String) AS cur
  UNION ALL
    SELECT e.nxt AS cur
    FROM enum_edges AS e
    INNER JOIN enum_walk AS w ON e.e = w.cur
)
SELECT cur FROM enum_walk ORDER BY cur
SETTINGS validate_enum_literals_in_operators = 1;

DROP TABLE enum_edges;

DROP TABLE edges;
DROP TABLE two_hop;
DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE pairs;
