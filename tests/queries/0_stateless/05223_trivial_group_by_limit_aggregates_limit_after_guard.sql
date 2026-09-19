-- The trivial `GROUP BY ... LIMIT` optimization must not fire for `LIMIT n AFTER cond` /
-- `LIMIT n UNTIL cond`: the range window is anchored at a boundary row that is only found
-- once the boundary key is produced, so capping the aggregation at `n + offset` arbitrary
-- groups can stop it before the boundary key appears and return an empty window while the
-- groups exist. Test `04343_limit_after_optimizer_guards` pins this for the settings-based
-- rewrite of aggregate-free projections; this test pins it for projections with aggregate
-- functions, where the planner applies the kept-keys cutoff through the shared helper.
--
-- The outer queries consume the inner aggregates, otherwise the analyzer removes them from
-- the projection and the planner path is not exercised. The key is `UInt64` on purpose: for
-- the fixed hash map methods (`UInt8`/`UInt16` keys) the cutoff is inert. Every key spans a
-- whole block (`intDiv(number, 100)` with `max_block_size = 100`), so a capped aggregation
-- keeps the first five keys and never produces the boundary key `k >= 50`.
--
-- `GROUP BY` output order is not deterministic, so the window is asserted through counts:
-- with all 100 groups computed the first row with `k >= 50` is always followed by the other
-- 49 keys above 50, so `LIMIT 5 AFTER` always yields exactly 5 rows. Without the guard in
-- `getTrivialGroupByLimit` the window is empty. (`LIMIT n UNTIL cond` takes the rows before
-- the boundary, whose number depends on the output order, so it cannot be pinned without an
-- `ORDER BY`, which by itself disables the optimization; the guard covers both forms.)

SET enable_analyzer = 1;
SET optimize_trivial_group_by_limit_query = 1;
SET max_threads = 4;
SET max_block_size = 100;

SELECT count(), sum(c) FROM (SELECT toUInt64(intDiv(number, 100)) AS k, count() AS c FROM numbers(10000) GROUP BY k LIMIT 5 AFTER k >= 50);
SELECT count(), sum(c), sum(s) > 0 FROM (SELECT toUInt64(intDiv(number, 100)) AS k, count() AS c, sum(number) AS s FROM numbers_mt(10000) GROUP BY k LIMIT 5 AFTER k >= 50);
