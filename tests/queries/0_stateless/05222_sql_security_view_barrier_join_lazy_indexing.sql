-- A `SQL SECURITY DEFINER` / `NONE` view whose plan contains a join is an optimization barrier
-- (a join can hide rows, so the whole view subplan is sealed). `optimizeJoinLazyIndexing` used to
-- walk from the invoker's `LIMIT` / `ORDER BY ... LIMIT` / follow-up `JOIN` down through the sealed
-- `Expression` / `Filter` steps without looking at the barrier flag and enable lazy column indexing
-- on the view's own join, so the invoker's plan shape retuned how much the join materializes over
-- the rows the view hides. Now the pass stops at the first barrier step it sees.

-- Whether lazy indexing fired is visible through `dumpColumnStructure`, which shows the probe-side
-- column of a lazily indexed hash join as `Replicated(...)` instead of materializing it first.
-- Only a hash join with unmatched probe rows in the block produces such a column, and the pass is
-- gated on the `LIMIT` value and the number of probe columns, so all of that is pinned (the block
-- size too, so that no block degenerates to a single matched row). For the `ORDER BY ... LIMIT`
-- shape the sort key depends on the dump, otherwise the projection is lifted above the sort and
-- sees the sorted, already materialized column.
SET enable_analyzer = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false',
    query_plan_min_columns_for_join_lazy_indexing = 1, query_plan_max_limit_for_join_lazy_indexing = 1000,
    enable_join_runtime_filters = 0, enable_parallel_replicas = 0, make_distributed_plan = 0, max_threads = 1,
    max_block_size = 65536;

DROP VIEW IF EXISTS v05222_invoker;
DROP VIEW IF EXISTS v05222_definer;
DROP TABLE IF EXISTS l05222;
DROP TABLE IF EXISTS r05222;
DROP TABLE IF EXISTS r05222_outer;
CREATE TABLE l05222 (k UInt64, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO l05222 SELECT number, repeat('x', 100) FROM numbers(200);
-- Only the even keys match, so the join drops half of the probe rows.
CREATE TABLE r05222 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO r05222 SELECT number * 2, number FROM numbers(100);
CREATE TABLE r05222_outer (k UInt64, z UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO r05222_outer SELECT number, number FROM numbers(200);

CREATE VIEW v05222_invoker SQL SECURITY INVOKER AS
    SELECT l05222.k AS k, l05222.payload AS payload, r05222.w AS w FROM l05222 JOIN r05222 ON l05222.k = r05222.k;
CREATE VIEW v05222_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT l05222.k AS k, l05222.payload AS payload, r05222.w AS w FROM l05222 JOIN r05222 ON l05222.k = r05222.k;

-- The `INVOKER` view stays fully optimizable. These are the positive controls proving that the
-- oracle discriminates: the invoker's `LIMIT`, `ORDER BY ... LIMIT` and follow-up `JOIN` all enable
-- lazy indexing on the view's join.
SELECT 'invoker, outer LIMIT:', dumpColumnStructure(payload) LIKE '%Replicated%' FROM v05222_invoker LIMIT 1;
SELECT 'invoker, outer ORDER BY LIMIT:', dumpColumnStructure(payload) LIKE '%Replicated%' AS lazy FROM v05222_invoker ORDER BY lazy, k LIMIT 1;
SELECT 'invoker, follow-up JOIN:', any(lazy)
FROM (SELECT k, dumpColumnStructure(payload) LIKE '%Replicated%' AS lazy FROM v05222_invoker) AS a
JOIN r05222_outer ON a.k = r05222_outer.k;

-- The `DEFINER` view is a barrier: the same outer shapes leave its join untouched.
SELECT 'definer, outer LIMIT:', dumpColumnStructure(payload) LIKE '%Replicated%' FROM v05222_definer LIMIT 1;
SELECT 'definer, outer ORDER BY LIMIT:', dumpColumnStructure(payload) LIKE '%Replicated%' AS lazy FROM v05222_definer ORDER BY lazy, k LIMIT 1;
SELECT 'definer, follow-up JOIN:', any(lazy)
FROM (SELECT k, dumpColumnStructure(payload) LIKE '%Replicated%' AS lazy FROM v05222_definer) AS a
JOIN r05222_outer ON a.k = r05222_outer.k;

-- The barrier only drops the optimization, never the result.
SELECT 'definer results:', groupArray(k) = [0, 2, 4], groupArray(w) = [0, 1, 2]
FROM (SELECT k, w FROM v05222_definer ORDER BY k LIMIT 3);
SELECT 'definer results through a follow-up JOIN:', count(), sum(z) = sum(k)
FROM v05222_definer AS a JOIN r05222_outer ON a.k = r05222_outer.k;

DROP VIEW v05222_invoker;
DROP VIEW v05222_definer;
DROP TABLE l05222;
DROP TABLE r05222;
DROP TABLE r05222_outer;
