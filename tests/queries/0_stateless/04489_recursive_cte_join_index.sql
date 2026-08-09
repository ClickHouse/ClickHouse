-- Tags: long
-- The `long` tag only lifts the 180s soft "runs too long" flaky-check warning; it
-- does NOT change the hard per-invocation `--timeout` (600s), which kills the whole
-- `.sql` file when one run exceeds it. This test has many distinct `WITH RECURSIVE`
-- queries (each a different planner edge case), and every recursive step rebuilds a
-- fresh pipeline. Under pathological random-settings combinations -- notably
-- `max_threads = 32` with split-range injection, per-stream read buffers and mmap
-- reads -- that per-step fixed cost multiplies across the whole file and, in the
-- debug build, pushed a single run past 600s. We therefore pin `max_threads` to a
-- small value below: the results proved here (recursive-walk outputs and the
-- primary-key mark-pruning `read_rows` counts) do not depend on thread count, so
-- capping it removes the dominant overhead while preserving all the coverage.

-- Test that recursive CTEs with JOINs use MergeTree primary key index.
-- Without the optimization, each recursion step scans the entire table.
-- With the optimization, join key values from the working table are injected
-- into the recursive step's WHERE clause as an `IN (...)` predicate, enabling
-- MergeTree primary key index usage.

SET enable_analyzer = 1;

-- Pin `max_threads` so the flaky check's random `max_threads = 32` draw cannot
-- multiply the per-step pipeline overhead into a >600s timeout (see the header
-- note). The data is tiny and the assertions are thread-count-independent.
SET max_threads = 4;

-- Disable JOIN runtime filters for the whole test. This optimization is
-- semantics-preserving but, with `enable_join_runtime_filters_index_analysis`,
-- it can prune a probe-side `MergeTree` scan through the primary-key index using
-- a filter built from the (small) working table's join keys. That is a *second*,
-- independent way to shrink `read_rows` besides this PR's `IN (...)` injection,
-- so it confounds every `read_rows` proof below: in particular the "optimization
-- disabled -> full scan -> high `read_rows`" fallback proofs would see the scan
-- pruned by the runtime filter instead and read far fewer rows. Pinning it off
-- isolates the measurement to the recursive-CTE `IN`-injection optimization that
-- is actually under test; the walk results are unaffected either way.
SET enable_join_runtime_filters = 0;
SET enable_join_runtime_filters_index_analysis = 0;

-- A small `index_granularity` plus `OPTIMIZE ... FINAL` gives a single part with
-- many marks, so the `read_rows` proof below is deterministic: it relies on
-- primary-key mark pruning within one part rather than on the two inserts happening
-- to stay in separate parts (a background merge could otherwise collapse them into
-- a single 8192-row mark and defeat part-level pruning). The chain keys (`from_id`
-- 0..9) all fall in the first mark, while the unrelated filler (`from_id` >= 1000)
-- fills later marks, so each recursive step's `from_id IN (...)` lookup reads only
-- that first mark.
DROP TABLE IF EXISTS edges;
CREATE TABLE edges
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 128;

-- Insert a chain: 0->1->2->...->9
INSERT INTO edges SELECT number, number + 1 FROM numbers(10);

-- Insert many unrelated rows to make index usage measurable.
-- from_id range [1000, 6000) has no connection to the chain above.
INSERT INTO edges SELECT number + 1000, number + 1000000 FROM numbers(5000);

OPTIMIZE TABLE edges FINAL;

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
-- context (`Planner::buildPlanForUnionNode` plans each branch with a child
-- `Planner` whose planner context is seeded from that branch's context, and
-- the branch's `CollectSets` runs under it). The set-limit preflight must
-- therefore use the containing branch's context, not the outer recursive one;
-- otherwise the strict branch would inject an oversized `IN` and throw
-- `SET_SIZE_LIMIT_EXCEEDED` / truncate the set.
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

-- The converse: the *outer* query carries the strict limit while the branches
-- carry a loose branch-local one. Because each branch is planned by its own
-- child `Planner` seeded from the branch's context, the branch-local
-- `max_rows_in_set = 10000` (which overrides the inherited outer `1`) is what
-- the planner materializes the injected `IN` under — so injection is safe and
-- the result must match the plain scan exactly (no `SET_SIZE_LIMIT_EXCEEDED`
-- under `'throw'`, no silent truncation under `'break'`). This pins down that
-- the preflight checking the branch's own context is *consistent* with the
-- planner: neither a stricter nor a looser outer limit may change the result.
WITH RECURSIVE walk_tree AS
(
    SELECT child AS current_id FROM tree WHERE parent = 0
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
    SETTINGS max_rows_in_set = 10000
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
    SETTINGS max_rows_in_set = 10000
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
    SETTINGS max_rows_in_set = 10000
  UNION ALL
    SELECT e.child AS current_id
    FROM tree AS e
    INNER JOIN walk_tree AS t ON e.parent = t.current_id
    SETTINGS max_rows_in_set = 10000
)
SELECT current_id FROM walk_tree ORDER BY current_id
SETTINGS max_rows_in_set = 1, set_overflow_mode = 'break';

DROP TABLE tree;

-- `recursive_cte_max_in_filter_cardinality` must be read from the containing
-- branch's context, not the synthetic recursive `UnionNode` context, for the
-- same reason as the set limits above. Two disjoint chains: `chain_big`
-- (0 -> ... -> 10, plus 5000 filler rows) and `chain_small`
-- (100 -> ... -> 110, no filler), each walked by its own recursive branch.
DROP TABLE IF EXISTS chain_big;
DROP TABLE IF EXISTS chain_small;
-- Small granularity + `OPTIMIZE ... FINAL` for the same determinism reason as
-- `edges`: the `walk_branch_cap2` proof below asserts a low `read_rows` for the
-- `chain_big` branch, which must come from mark pruning inside one part, not from
-- the two inserts staying in separate parts.
CREATE TABLE chain_big (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 128;
CREATE TABLE chain_small (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id;
INSERT INTO chain_big SELECT number, number + 1 FROM numbers(10);
INSERT INTO chain_big SELECT number + 1000, number + 1000000 FROM numbers(5000);
INSERT INTO chain_small SELECT number + 100, number + 101 FROM numbers(10);
OPTIMIZE TABLE chain_big FINAL;

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
-- Small granularity + `OPTIMIZE ... FINAL` for the same determinism reason as
-- the other `read_rows` proofs: the `walk_conv` assertion below must come from
-- mark pruning inside one part, not from the two inserts staying in separate
-- parts, which a background merge could otherwise collapse into a single mark.
CREATE TABLE conv_edges (parent UInt16, child UInt16) ENGINE = MergeTree ORDER BY parent SETTINGS index_granularity = 128;
INSERT INTO conv_edges VALUES (0, 1), (1, 2);
INSERT INTO conv_edges SELECT number + 1000, number + 30000 FROM numbers(5000);
OPTIMIZE TABLE conv_edges FINAL;

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

-- A fail-closed fallback for one join key must be scoped to that key, not the
-- whole recursive step: each generated predicate is independently
-- semantics-preserving, so a branch that cannot be safely optimized must not
-- disable the injected `IN` filter of an unrelated, safe branch. Here two
-- recursive branches share one CTE. The `big` branch walks a large, prunable
-- table (its `from_id IN (...)` lookup must keep hitting the index), while the
-- `small` branch carries a branch-local `max_rows_in_set = 1`: because the
-- shared frontier holds two live chains, the `small` branch's generated set is
-- always oversized, so its preflight (`generatedInSetIsSafeToInject`) fails and
-- that branch correctly falls back to a plain scan every step. That fallback
-- must skip only the `small` key; if it discarded the whole step's predicates,
-- the safe `big.from_id IN (...)` filter would be dropped too and `big` (5010
-- rows) full-scanned on every step. The result is identical either way (the
-- optimization is semantics-preserving), so only `read_rows` proves the safe
-- branch stayed indexed.
DROP TABLE IF EXISTS big_branch;
DROP TABLE IF EXISTS small_branch;
CREATE TABLE big_branch (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 128;
INSERT INTO big_branch SELECT number, number + 1 FROM numbers(10);
INSERT INTO big_branch SELECT number + 1000, number + 1000000 FROM numbers(5000);
OPTIMIZE TABLE big_branch FINAL;
CREATE TABLE small_branch (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id;
INSERT INTO small_branch SELECT number + 100, number + 101 FROM numbers(10);

WITH RECURSIVE mixed_branch_skip AS
(
    SELECT CAST(number, 'UInt64') * 100 AS id FROM numbers(2)
  UNION ALL
    SELECT b.to_id AS id
    FROM big_branch AS b
    INNER JOIN mixed_branch_skip AS r ON b.from_id = r.id
  UNION ALL
    SELECT s.to_id AS id
    FROM small_branch AS s
    INNER JOIN mixed_branch_skip AS r ON s.from_id = r.id
    SETTINGS max_rows_in_set = 1, set_overflow_mode = 'throw'
)
SELECT count() FROM mixed_branch_skip;

SYSTEM FLUSH LOGS query_log;

-- The safe `big_branch` must stay indexed even though `small_branch` falls back
-- on every step: a per-key skip keeps `big_branch` pruned (a few hundred rows
-- total — the trace shows a single 1/39 mark read per step), whereas a
-- whole-step abort would full-scan it (~50K rows).
SELECT read_rows < 10000 AS safe_branch_stays_indexed
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE mixed_branch_skip%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE big_branch;
DROP TABLE small_branch;

-- A byte-heavy generated `IN` set with a tiny `max_bytes_in_set` must fail
-- closed to a plain scan, exactly like the row-count guard above. The working
-- frontier holds wide `String` keys, so the generated set's measured byte size
-- dwarfs `max_bytes_in_set = 1`; injecting it would throw
-- `SET_SIZE_LIMIT_EXCEEDED` (`set_overflow_mode = 'throw'`), so the optimization
-- skips injection for the step and scans plainly, still traversing the chain
-- correctly. Building the probe set that makes this decision must itself never
-- throw (e.g. it can hit `max_memory_usage` while materializing many wide
-- keys); the unoptimized scan never builds that set, so a failure there falls
-- back to the plain scan rather than failing the query.
-- Small granularity + `OPTIMIZE ... FINAL` so the positive (normal-limit) proof
-- just below is deterministic: the wide `'k'` chain keys all fall in the first
-- mark of a single part and the `'z'` filler fills later marks, so an indexed
-- lookup reads only the first mark instead of relying on part-level pruning.
DROP TABLE IF EXISTS str_chain;
CREATE TABLE str_chain (cur String, nxt String) ENGINE = MergeTree ORDER BY cur SETTINGS index_granularity = 128;
INSERT INTO str_chain
    SELECT repeat('k', 200) || toString(number) AS cur,
           repeat('k', 200) || toString(number + 1) AS nxt
    FROM numbers(6);
-- Unrelated wide-key filler rows so a full scan is measurably large. They are
-- isolated self-loops (`cur = nxt`) never reached from the `'k'` chain seed, so
-- the result is unchanged; their only purpose is to make the fallback's
-- `read_rows` observable.
INSERT INTO str_chain
    SELECT repeat('z', 200) || toString(number) AS cur,
           repeat('z', 200) || toString(number) AS nxt
    FROM numbers(5000);
OPTIMIZE TABLE str_chain FINAL;

-- Positive counterpart to the `max_bytes_in_set = 1` fallback below: with the
-- default (unlimited) set-size limits the wide `String` join keys are pushed into
-- the index, so each recursive step reads only the first mark holding the `'k'`
-- chain, not the `'z'` filler. This proves the String-key optimization actually
-- fires in the normal case; the small-limit case that follows proves the fallback.
WITH RECURSIVE str_walk_opt AS
(
    SELECT repeat('k', 200) || '0' AS cur
  UNION ALL
    SELECT e.nxt AS cur
    FROM str_chain AS e
    INNER JOIN str_walk_opt AS w ON e.cur = w.cur
)
SELECT count() FROM str_walk_opt;

SYSTEM FLUSH LOGS query_log;

SELECT read_rows < 10000 AS str_key_optimized
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE str_walk_opt%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

WITH RECURSIVE str_walk AS
(
    SELECT repeat('k', 200) || '0' AS cur
  UNION ALL
    SELECT e.nxt AS cur
    FROM str_chain AS e
    INNER JOIN str_walk AS w ON e.cur = w.cur
)
SELECT count() FROM str_walk
SETTINGS max_bytes_in_set = 1, set_overflow_mode = 'throw';

SYSTEM FLUSH LOGS query_log;

-- With `max_bytes_in_set = 1` the generated set's bytes dwarf the limit, so the
-- optimization falls back to a plain scan on every recursive step: the whole
-- table (chain + filler) is read each step and `read_rows` is far above the
-- handful of rows an index lookup of the chain keys would touch. This proves the
-- fallback actually fired before injecting an oversized set, not merely that the
-- result is correct. The generated values are bounded *while being collected*,
-- so a frontier of wide keys never materializes the full set / RHS tuple before
-- this fallback decision.
SELECT read_rows > 10000 AS byte_limit_fallback_full_scan
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE str_walk%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE str_chain;

-- Forced parallel replicas (`allow_experimental_parallel_reading_from_replicas
-- = 2`) cannot be satisfied for the recursive part of a recursive CTE: every
-- recursive step disables parallel replicas to avoid reusing a stale cached
-- GLOBAL JOIN table (which would return wrong results). The forcing mode is
-- documented as "enabled, throw an exception in case of failure", so the query
-- must fail closed with `SUPPORT_IS_DISABLED` rather than silently run without
-- the requested parallel replicas. The rejection is gated on parallel replicas
-- actually being usable (`max_parallel_replicas > 1` etc.), matching every
-- other forced-mode rejection in the planner; mode `1` (best-effort, silent
-- fallback) is exercised by the other queries above.
WITH RECURSIVE traverse_pr AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e
    INNER JOIN traverse_pr AS t ON e.from_id = t.current_id
)
SELECT current_id FROM traverse_pr ORDER BY current_id
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- The same force-or-throw contract must hold for *every* parallel-replica mode
-- the recursive context could otherwise engage, not just the task-based one.
-- A forced custom-key mode (`parallel_replicas_mode = 'custom_key_sampling'`)
-- is not covered by `canUseTaskBasedParallelReplicas`; without checking the
-- custom-key / offset predicates as well it would be silently downgraded here
-- instead of failing closed. It must still raise `SUPPORT_IS_DISABLED`.
WITH RECURSIVE traverse_pr_custom_key AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e
    INNER JOIN traverse_pr_custom_key AS t ON e.from_id = t.current_id
)
SELECT current_id FROM traverse_pr_custom_key ORDER BY current_id
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'from_id',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Conversely, the forced-mode rejection must stay as narrow as the planner's own
-- decision: a recursive CTE whose recursive part reads no parallel-replica-eligible
-- table (here, only the in-memory working table) would never engage parallel replicas
-- anyway, so it must keep running under the forcing mode instead of failing closed.
WITH RECURSIVE self_only_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM self_only_pr WHERE n < 10
)
SELECT sum(n) FROM self_only_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- The same, with a forced custom-key mode: still no eligible table, still no throw.
WITH RECURSIVE self_only_pr_custom_key AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM self_only_pr_custom_key WHERE n < 10
)
SELECT sum(n) FROM self_only_pr_custom_key
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'n',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- A recursive part that joins a real `MergeTree` table still fails closed under the
-- forcing mode even when the recursion is otherwise trivial — the eligible table is
-- what makes the request unsatisfiable, and it is detected anywhere in the join tree.
WITH RECURSIVE joined_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM joined_pr AS t INNER JOIN edges AS e ON e.from_id = t.n WHERE n < 10
)
SELECT sum(n) FROM joined_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- The rejection is decided per branch context, not once for the whole recursive query:
-- a branch that carries the forcing mode in its own `SETTINGS` clause but reads nothing
-- but the working table must keep running, even when a sibling branch reads a
-- `MergeTree` table with parallel replicas disabled for itself.
WITH RECURSIVE mixed_branch_pr AS
(
    SELECT toUInt64(1) AS n
  UNION ALL
    SELECT n + 1 FROM mixed_branch_pr WHERE n < 10
    SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
        parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0
  UNION ALL
    SELECT e.to_id FROM edges AS e INNER JOIN mixed_branch_pr AS t ON e.from_id = t.n WHERE t.n > 1000000
    SETTINGS allow_experimental_parallel_reading_from_replicas = 0
)
SELECT sum(n) FROM mixed_branch_pr;

-- Conversely, the branch that does read the `MergeTree` table still fails closed when
-- the forcing mode is set in its own `SETTINGS` clause.
WITH RECURSIVE mixed_branch_pr_throw AS
(
    SELECT toUInt64(1) AS n
  UNION ALL
    SELECT n + 1 FROM mixed_branch_pr_throw WHERE n < 10
    SETTINGS allow_experimental_parallel_reading_from_replicas = 0
  UNION ALL
    SELECT e.to_id FROM edges AS e INNER JOIN mixed_branch_pr_throw AS t ON e.from_id = t.n WHERE t.n > 1000000
    SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
        parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0
)
SELECT sum(n) FROM mixed_branch_pr_throw; -- { serverError SUPPORT_IS_DISABLED }

-- The rejection must be no broader than the planner's own storage-level eligibility rule
-- (`canUseTableForParallelReplicas`). A plain, non-replicated local `MergeTree` table with
-- `parallel_replicas_for_non_replicated_merge_tree = 0` is not eligible: the planner would
-- never engage parallel replicas for it, so the forcing mode has nothing to fail on and the
-- query must keep running. Every throwing case above therefore has to set
-- `parallel_replicas_for_non_replicated_merge_tree = 1` explicitly; this is the converse.
-- The setting is spelled out here as well, because the test harness randomizes it to `1`.
WITH RECURSIVE joined_pr_non_replicated AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM joined_pr_non_replicated AS t INNER JOIN edges AS e ON e.from_id = t.n WHERE n < 10
)
SELECT sum(n) FROM joined_pr_non_replicated
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 0, automatic_parallel_replicas_mode = 0;

-- A view over a `MergeTree` table can engage parallel replicas regardless of
-- `parallel_replicas_allow_view_over_mergetree`: that setting only gates the *outer*
-- planner's unwrapping of the view, while with the setting off `StorageView::readImpl`
-- still re-interprets the inner query with the reading context's settings, and that inner
-- planner engages parallel replicas for the bare eligible `MergeTree` (the plain read's
-- plan contains `ReadFromRemoteParallelReplicas` over the view's inner query either way).
-- The forced mode must therefore fail closed with the view support turned off too ...
CREATE VIEW edges_view AS SELECT * FROM edges;

WITH RECURSIVE view_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM view_pr AS t INNER JOIN edges_view AS e ON e.from_id = t.n WHERE n < 10
)
SELECT sum(n) FROM view_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 0,
    automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- ... and with it on, when the outer planner itself can read the view with parallel replicas.
WITH RECURSIVE view_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM view_pr_throw AS t INNER JOIN edges_view AS e ON e.from_id = t.n WHERE n < 10
)
SELECT sum(n) FROM view_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 1,
    automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP VIEW edges_view;

-- A remote read is not eligible either unless the cluster it goes to has a shard with more
-- than one node: `ClusterProxy::updateSettingsAndClientInfoForCluster` turns task-based
-- parallel replicas off for a single-node cluster and for a `remote()` table function without
-- a named cluster, so such a query runs as a plain remote read and must keep running under
-- the forcing mode.
WITH RECURSIVE remote_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM remote_pr AS t
        INNER JOIN remote('127.0.0.1', currentDatabase(), edges) AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM remote_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- ... while a cluster with several replicas in a shard really can be read with parallel
-- replicas, so there the forcing mode must be rejected.
WITH RECURSIVE cluster_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM cluster_pr_throw AS t
        INNER JOIN cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), edges) AS e
            ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM cluster_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- A `Distributed` table over a cluster that mixes single-node and multi-node shards may be
-- shrunk by shard pruning before the read path decides on parallel replicas:
-- `StorageDistributed::getQueryProcessingStage` applies `optimize_skip_unused_shards` first,
-- and when the pruned cluster keeps single-node shards only, the read silently runs without
-- parallel replicas. Which shards survive depends on the query's WHERE clause and is unknown
-- before planning, so the forcing mode must not reject such a query up front. Both shards
-- point back at this very server, so the plain read sees two copies of every edge —
-- `sum(DISTINCT n)` keeps the expected result independent of that.
DROP TABLE IF EXISTS edges_dist;
CREATE TABLE edges_dist AS edges
    ENGINE = Distributed('test_cluster_mixed_replica_count_localhost', currentDatabase(), edges, from_id);

WITH RECURSIVE pruned_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM pruned_pr AS t INNER JOIN edges_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(DISTINCT n) FROM pruned_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    optimize_skip_unused_shards = 1;

-- Without shard pruning the read goes to the full mixed cluster, whose multi-node shard
-- really can engage parallel replicas, so there the forcing mode must still be rejected.
WITH RECURSIVE pruned_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM pruned_pr_throw AS t INNER JOIN edges_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(DISTINCT n) FROM pruned_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    optimize_skip_unused_shards = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_dist;

-- The forced-mode rejection is a positive capability check: among remote storages, only
-- reads served by `ClusterProxy` (`Distributed` tables and the `remote` / `cluster` /
-- `clusterAllReplicas` table functions) consult the parallel-replica settings at all. A
-- cluster table function (`urlCluster` here) distributes its read by itself, never reads
-- `allow_experimental_parallel_reading_from_replicas`, and so cannot be downgraded by the
-- recursive-step disable — the forcing mode must not reject it, even though the cluster it
-- reads over has a shard with several replicas. The same goes for remote engines with a
-- direct read path (`MongoDB`, `YTsaurus`, ...), which cannot be exercised in a stateless
-- test.
WITH RECURSIVE cluster_fn_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + toUInt64(u.one) FROM cluster_fn_pr AS t
        INNER JOIN urlCluster('test_cluster_one_shard_three_replicas_localhost',
            'http://localhost:8123/?query=select+1+format+TSV', 'TSV', 'one UInt8') AS u
            ON u.one = toUInt8(t.n > 0)
    WHERE n < 10
)
SELECT sum(n) FROM cluster_fn_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- A table defined `AS cluster(...)` is a lazy proxy (`StorageProxy`) over the underlying
-- `Distributed` storage; the capability check unwraps it, so the forcing mode is still
-- rejected when the proxied read really can engage parallel replicas.
DROP TABLE IF EXISTS edges_cluster_proxy;
CREATE TABLE edges_cluster_proxy AS cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), edges);

WITH RECURSIVE proxy_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM proxy_pr_throw AS t INNER JOIN edges_cluster_proxy AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM proxy_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_cluster_proxy;

-- A materialized view reads its target table directly, so a view targeting a `Distributed`
-- table over a multi-replica cluster is unwrapped the same way and still fails closed under
-- the forcing mode.
DROP VIEW IF EXISTS edges_dist_mv;
DROP TABLE IF EXISTS edges_dist_replicas;
CREATE TABLE edges_dist_replicas AS edges
    ENGINE = Distributed('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), edges);
CREATE MATERIALIZED VIEW edges_dist_mv TO edges_dist_replicas AS SELECT * FROM edges;

WITH RECURSIVE mv_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM mv_pr_throw AS t INNER JOIN edges_dist_mv AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM mv_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP VIEW edges_dist_mv;

-- A `Buffer` table forwards its read to the destination table, so a `Buffer` over a local
-- table cannot engage parallel replicas (the planner sees the `Buffer`, which is not eligible)
-- and must keep running under the forcing mode.
DROP TABLE IF EXISTS edges_buffer_local;
CREATE TABLE edges_buffer_local AS edges
    ENGINE = Buffer(currentDatabase(), edges, 1, 10, 100, 10, 100, 10000, 100000);

WITH RECURSIVE buffer_local_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM buffer_local_pr AS t INNER JOIN edges_buffer_local AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM buffer_local_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

DROP TABLE edges_buffer_local;

-- A `Buffer` over a `Distributed` table forwards the read to that remote destination, which
-- can engage parallel replicas, so the forcing mode must fail closed instead of silently
-- downgrading to a plain read.
DROP TABLE IF EXISTS edges_buffer_dist;
CREATE TABLE edges_buffer_dist AS edges
    ENGINE = Buffer(currentDatabase(), edges_dist_replicas, 1, 10, 100, 10, 100, 10000, 100000);

WITH RECURSIVE buffer_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM buffer_pr_throw AS t INNER JOIN edges_buffer_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM buffer_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_buffer_dist;

-- A `Merge` table whose children are all local cannot engage parallel replicas (the
-- storage-level `MergeTree` parallel-replica paths are old-analyzer-only, and the planner
-- rejects `Merge` itself), so it must keep running under the forcing mode.
DROP TABLE IF EXISTS edges_merge_local;
CREATE TABLE edges_merge_local AS edges ENGINE = Merge(currentDatabase(), '^edges$');

WITH RECURSIVE merge_local_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM merge_local_pr AS t INNER JOIN edges_merge_local AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM merge_local_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

DROP TABLE edges_merge_local;

-- A `Merge` table plans each child with the same query context, so a `Distributed` child
-- read still goes through `ClusterProxy` and can engage parallel replicas — the forcing
-- mode must fail closed instead of silently downgrading to a plain read.
DROP TABLE IF EXISTS edges_merge_dist;
CREATE TABLE edges_merge_dist AS edges ENGINE = Merge(currentDatabase(), '^edges_dist_replicas$');

WITH RECURSIVE merge_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM merge_pr_throw AS t INNER JOIN edges_merge_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM merge_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_merge_dist;

-- A `Merge` table mixing local and remote children prunes the child set per query
-- (`_table` / `_database` filters), so a recursive step narrowed to the local child never
-- reaches the remote one — the forcing mode must keep running instead of rejecting the
-- query for a remote child it would not read.
DROP TABLE IF EXISTS edges_merge_mixed;
CREATE TABLE edges_merge_mixed AS edges ENGINE = Merge(currentDatabase(), '^edges(_dist_replicas)?$');

WITH RECURSIVE merge_mixed_local_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM merge_mixed_local_pr AS t INNER JOIN edges_merge_mixed AS e ON e.from_id = t.n
    WHERE n < 10 AND e._table = 'edges'
)
SELECT sum(n) FROM merge_mixed_local_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

DROP TABLE edges_merge_mixed;

-- An ordinary `VIEW` over a local table cannot engage parallel replicas (with
-- `parallel_replicas_for_non_replicated_merge_tree = 0` — pinned, the harness may randomize
-- it — the inner `MergeTree` is not eligible either way), so it must keep running under the
-- forcing mode.
DROP VIEW IF EXISTS edges_view_local;
CREATE VIEW edges_view_local AS SELECT * FROM edges;

WITH RECURSIVE view_local_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM view_local_pr AS t INNER JOIN edges_view_local AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM view_local_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 0, automatic_parallel_replicas_mode = 0;

DROP VIEW edges_view_local;

-- An ordinary non-inlined `VIEW` re-interprets its inner query with the reading context's
-- settings, so a view over a `Distributed` table still reaches `ClusterProxy` and can engage
-- parallel replicas even though the view storage itself is not remote — the forcing mode
-- must fail closed instead of silently downgrading to a plain read.
DROP VIEW IF EXISTS edges_view_dist;
CREATE VIEW edges_view_dist AS SELECT * FROM edges_dist_replicas;

WITH RECURSIVE view_dist_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM view_dist_pr_throw AS t INNER JOIN edges_view_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM view_dist_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP VIEW edges_view_dist;

-- The `view` table function is a `StorageView` too and must be checked the same way.
WITH RECURSIVE view_fn_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM view_fn_pr_throw AS t
    INNER JOIN view(SELECT * FROM edges_dist_replicas) AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM view_fn_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- A delegating wrapper whose target is an ordinary `VIEW` must be judged with the view rule:
-- `StorageView` is not remote, so an `isRemote` gate on the unwrapped target would silently
-- miss it. A `Merge` over a view over a local table cannot engage parallel replicas (with
-- `parallel_replicas_for_non_replicated_merge_tree = 0` — pinned, the harness may randomize
-- it — the inner `MergeTree` is not eligible either way), so it must keep running under the
-- forcing mode.
DROP VIEW IF EXISTS edges_view_wrapped;
DROP TABLE IF EXISTS edges_merge_view_local;
CREATE VIEW edges_view_wrapped AS SELECT * FROM edges;
CREATE TABLE edges_merge_view_local AS edges ENGINE = Merge(currentDatabase(), '^edges_view_wrapped$');

WITH RECURSIVE merge_view_local_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM merge_view_local_pr AS t INNER JOIN edges_merge_view_local AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM merge_view_local_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE edges_merge_view_local;
DROP VIEW edges_view_wrapped;

-- A `Merge` over a view over a `Distributed` table reads the view with the same query
-- context, and the view re-interprets its inner query, which reaches `ClusterProxy` and
-- can engage parallel replicas — the forcing mode must fail closed instead of silently
-- downgrading to a plain read.
DROP VIEW IF EXISTS edges_view_wrapped_dist;
DROP TABLE IF EXISTS edges_merge_view_dist;
CREATE VIEW edges_view_wrapped_dist AS SELECT * FROM edges_dist_replicas;
CREATE TABLE edges_merge_view_dist AS edges ENGINE = Merge(currentDatabase(), '^edges_view_wrapped_dist$');

WITH RECURSIVE merge_view_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM merge_view_pr_throw AS t INNER JOIN edges_merge_view_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM merge_view_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_merge_view_dist;

-- An `Alias` table over such a view forwards the read to it the same way.
DROP TABLE IF EXISTS edges_alias_view_dist;
CREATE TABLE edges_alias_view_dist ENGINE = Alias(currentDatabase(), 'edges_view_wrapped_dist');

WITH RECURSIVE alias_view_pr_throw AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM alias_view_pr_throw AS t INNER JOIN edges_alias_view_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM alias_view_pr_throw
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_alias_view_dist;
DROP VIEW edges_view_wrapped_dist;

DROP TABLE edges_dist_replicas;

DROP TABLE edges;
DROP TABLE two_hop;
DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE pairs;

-- The frontier deduplication must distinguish values by their raw
-- representation, the way the hash `JOIN` and the generated `IN` set do —
-- not by SQL-style `Field` comparison, which collapses `+0.` with `-0.` (and
-- all NaN payloads). The seed produces both `+0.` and `-0.`; each joins a
-- different edge row (a hash join matches float keys on raw bits, so `+0.`
-- only matches the `+0.` row and `-0.` only the `-0.` row). If the dedup
-- collapsed them, the injected `IN` prefilter would contain only one of the
-- two zeros and silently drop the other branch's edge row before the join.
-- Ordering by the raw bits keeps `+0.` and `-0.` in a deterministic order.
DROP TABLE IF EXISTS float_edges;
CREATE TABLE float_edges (from_id Float64, to_id Float64) ENGINE = MergeTree ORDER BY from_id;
INSERT INTO float_edges VALUES (0., 1.), (-0., 2.);

WITH RECURSIVE float_walk AS
(
    SELECT arrayJoin([toFloat64(0.), toFloat64(-0.)]) AS cur
  UNION ALL
    SELECT e.to_id AS cur
    FROM float_edges AS e
    INNER JOIN float_walk AS w ON e.from_id = w.cur
)
SELECT cur FROM float_walk ORDER BY reinterpretAsUInt64(cur)
SETTINGS join_algorithm = 'hash'; -- pinned: this proof is about raw-bit hash-join semantics

DROP TABLE float_edges;

-- The sort/merge-based join algorithms (`full_sorting_merge`,
-- `parallel_full_sorting_merge`, `partial_merge`,
-- `prefer_partial_merge`, and `auto`, which may fall back to `partial_merge`)
-- compare floating-point keys by value: `+0.` equals `-0.` and all NaNs are
-- equal. The generated `IN` prefilter matches on the raw representation
-- (`-0. IN (0.)` is `0`), so injecting it under such an algorithm would filter
-- out the `-0.`-keyed edge row that the join itself (probed with the `+0.`
-- frontier value) still matches — silently losing the rest of the recursion
-- (only the seed `0` would be returned instead of `0, 5, 7`). The optimization
-- must fail closed for floating-point keys under these algorithms: plain scan,
-- complete results.
DROP TABLE IF EXISTS float_edges_mj;
CREATE TABLE float_edges_mj (from_id Float64, to_id Float64) ENGINE = MergeTree ORDER BY from_id;
INSERT INTO float_edges_mj VALUES (-0., 5.), (5., 7.);

WITH RECURSIVE float_walk_fsm AS
(
    SELECT toFloat64(0.) AS cur
  UNION ALL
    SELECT e.to_id AS cur
    FROM float_edges_mj AS e
    INNER JOIN float_walk_fsm AS w ON e.from_id = w.cur
)
SELECT cur FROM float_walk_fsm ORDER BY cur
SETTINGS join_algorithm = 'full_sorting_merge';

WITH RECURSIVE float_walk_pm AS
(
    SELECT toFloat64(0.) AS cur
  UNION ALL
    SELECT e.to_id AS cur
    FROM float_edges_mj AS e
    INNER JOIN float_walk_pm AS w ON e.from_id = w.cur
)
SELECT cur FROM float_walk_pm ORDER BY cur
SETTINGS join_algorithm = 'partial_merge';

-- `parallel_full_sorting_merge` builds the very same `FullSortingMergeJoin` as
-- `full_sorting_merge`, so it compares floating-point keys by value too and
-- must fail closed identically.
WITH RECURSIVE float_walk_pfsm AS
(
    SELECT toFloat64(0.) AS cur
  UNION ALL
    SELECT e.to_id AS cur
    FROM float_edges_mj AS e
    INNER JOIN float_walk_pfsm AS w ON e.from_id = w.cur
)
SELECT cur FROM float_walk_pfsm ORDER BY cur
SETTINGS join_algorithm = 'parallel_full_sorting_merge';

DROP TABLE float_edges_mj;

-- Converse proofs on a larger table: a floating-point join key stays optimized
-- under a hash-family algorithm (raw-bit semantics match the `IN`, low
-- `read_rows`), and falls back to a plain scan under a value-comparing one
-- (high `read_rows`) — with identical, complete results in both cases.
DROP TABLE IF EXISTS float_chain;
CREATE TABLE float_chain (from_id Float64, to_id Float64) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 128;
INSERT INTO float_chain SELECT number, number + 1 FROM numbers(10);
INSERT INTO float_chain SELECT number + 1000, number + 1000000 FROM numbers(5000);
OPTIMIZE TABLE float_chain FINAL;

WITH RECURSIVE float_traverse_hash AS
(
    SELECT to_id AS current_id
    FROM float_chain
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM float_chain AS e
    INNER JOIN float_traverse_hash AS t ON e.from_id = t.current_id
)
SELECT current_id FROM float_traverse_hash ORDER BY current_id
SETTINGS join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;

SELECT
    read_rows < 10000 AS float_key_hash_join_optimized
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE float_traverse_hash%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

WITH RECURSIVE float_traverse_merge AS
(
    SELECT to_id AS current_id
    FROM float_chain
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM float_chain AS e
    INNER JOIN float_traverse_merge AS t ON e.from_id = t.current_id
)
SELECT current_id FROM float_traverse_merge ORDER BY current_id
SETTINGS join_algorithm = 'full_sorting_merge';

SYSTEM FLUSH LOGS query_log;

SELECT
    read_rows > 10000 AS float_key_merge_join_plain_scan
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE float_traverse_merge%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE float_chain;

-- With `parallel_replicas_plan_based = 1` the recursive-step parallel-replica
-- disable does not apply: the stale cached `GLOBAL JOIN` table it guards
-- against is built only by the legacy SQL-shipping construction
-- (`rewriteJoinToGlobalJoin` + `buildQueryTreeForShard`, which reuses the
-- materialized working table by tree hash across steps), and the plan-based
-- mode never runs that construction — it distributes a serialized plan
-- fragment that ships the current external tables with every step instead.
-- So a recursive join under the forcing mode must *run* (with correct
-- multi-step results), not fail closed: the walk below only reaches 10 if
-- every step joins against the fresh working table rather than a stale one.
DROP TABLE IF EXISTS edges_pb;
CREATE TABLE edges_pb
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 128;

INSERT INTO edges_pb SELECT number, number + 1 FROM numbers(10);
INSERT INTO edges_pb SELECT number + 1000, number + 1000000 FROM numbers(5000);

OPTIMIZE TABLE edges_pb FINAL;

WITH RECURSIVE plan_based_traverse_pr AS
(
    SELECT to_id AS current_id
    FROM edges_pb
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges_pb AS e
    INNER JOIN plan_based_traverse_pr AS t ON e.from_id = t.current_id
)
SELECT current_id FROM plan_based_traverse_pr ORDER BY current_id
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_plan_based = 1, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- The best-effort mode (`= 1`) must likewise keep plan-based parallelism
-- enabled for the recursive steps instead of downgrading them to plain runs.
WITH RECURSIVE plan_based_joined_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM plan_based_joined_pr AS t INNER JOIN edges_pb AS e ON e.from_id = t.n WHERE n < 10
)
SELECT sum(n) FROM plan_based_joined_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_plan_based = 1, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

DROP TABLE edges_pb;

-- Under the forcing mode the planner itself does not always throw: with
-- `parallel_replicas_min_number_of_rows_per_replica > 0` a task-based read from a local
-- `MergeTree` table first estimates the rows to read and *silently disables* parallel
-- replicas when the estimate is below the threshold, even with
-- `allow_experimental_parallel_reading_from_replicas = 2`. A small recursive step over such
-- a table would run plainly on the non-recursive path, so the recursive-step rejection must
-- not fire preemptively when that later estimate could still disable parallel replicas —
-- the step falls back to a plain run instead (mirroring the planner's own silent disable).
DROP TABLE IF EXISTS edges_minrows;
CREATE TABLE edges_minrows
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id;

INSERT INTO edges_minrows SELECT number, number + 1 FROM numbers(10);

WITH RECURSIVE minrows_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM minrows_pr AS t INNER JOIN edges_minrows AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM minrows_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_min_number_of_rows_per_replica = 1000000;

-- Conversely, the row-count estimate never runs for a read served through `ClusterProxy`
-- (a `Distributed` table), so for a remote-eligible recursive step the threshold cannot
-- disable parallel replicas later and the forcing mode must still fail closed, even with
-- `parallel_replicas_min_number_of_rows_per_replica` set.
DROP TABLE IF EXISTS edges_minrows_dist;
CREATE TABLE edges_minrows_dist AS cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), edges_minrows);

WITH RECURSIVE minrows_dist_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM minrows_dist_pr AS t INNER JOIN edges_minrows_dist AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM minrows_dist_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_min_number_of_rows_per_replica = 1000000; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_minrows_dist;

-- The estimate applies only to the task-based mode: a forced custom-key mode never runs it,
-- so it must keep failing closed regardless of the threshold.
WITH RECURSIVE minrows_custom_key_pr AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM minrows_custom_key_pr AS t INNER JOIN edges_minrows AS e ON e.from_id = t.n
    WHERE n < 10
)
SELECT sum(n) FROM minrows_custom_key_pr
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'from_id',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_min_number_of_rows_per_replica = 1000000; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE edges_minrows;

-- The working table must be visible — and current — through *every* context of the
-- recursive query tree, not only the root's: every `QueryNode`/`UnionNode` has its own
-- `Context` copy, and `Context::getExternalTables` overlays the node-local mapping over the
-- query context, never over an intermediate parent. Plan-based parallel replicas ship
-- `getExternalTables()` of the context that planned each fragment to the remote replicas,
-- so a branch with its own context (a multi-branch recursive part, or a branch-local
-- `SETTINGS` clause) must see the frontier of the current step there. These walks only
-- reach 10 if every step joins against the fresh working table.
DROP TABLE IF EXISTS edges_pb_branches;
CREATE TABLE edges_pb_branches
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 128;

INSERT INTO edges_pb_branches SELECT number, number + 1 FROM numbers(10);
INSERT INTO edges_pb_branches SELECT number + 1000, number + 1000000 FROM numbers(5000);

OPTIMIZE TABLE edges_pb_branches FINAL;

WITH RECURSIVE plan_based_branch_settings_pr AS
(
    SELECT to_id AS current_id
    FROM edges_pb_branches
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges_pb_branches AS e
    INNER JOIN plan_based_branch_settings_pr AS t ON e.from_id = t.current_id
    SETTINGS max_block_size = 65409
)
SELECT current_id FROM plan_based_branch_settings_pr ORDER BY current_id
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_plan_based = 1, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

WITH RECURSIVE plan_based_multi_branch_pr AS
(
    SELECT to_id AS current_id
    FROM edges_pb_branches
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges_pb_branches AS e
    INNER JOIN plan_based_multi_branch_pr AS t ON e.from_id = t.current_id AND t.current_id < 5
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges_pb_branches AS e
    INNER JOIN plan_based_multi_branch_pr AS t ON e.from_id = t.current_id AND t.current_id >= 5
)
SELECT current_id FROM plan_based_multi_branch_pr ORDER BY current_id
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_plan_based = 1, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

DROP TABLE edges_pb_branches;
