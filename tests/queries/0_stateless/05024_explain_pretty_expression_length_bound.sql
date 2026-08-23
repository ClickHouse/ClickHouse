-- Tags: no-flaky-check
--
-- A deep chain where each level references the previous column three times. The ActionsDAG stays
-- small, but rendering it as a tree emitted 3^N characters and the server was OOM-killed.
--
-- no-flaky-check: every arm is a deliberately expensive plan, and the repeated-run harness both
-- multiplies that cost and holds the result to a 180s per-test limit. The work is the same each run.

-- Asserted on the rendered expression, with the `indent + label` prefix stripped, because the limit
-- bounds one expression and a plan line is `indent + label + expression` (and may carry several
-- expressions). 8195 is MAX_EXPRESSION_LENGTH + length(TRUNCATED_MARKER) in QueryPlanFormat.cpp.

SET explain_query_plan_default = 'pretty';
-- Pinned: without the analyzer the alias chains are expanded before planning, so the deepest arms are
-- rejected as too big and the runtime filter never reaches the source filter these bounds are about.
SET enable_analyzer = 1;

-- The rendered arithmetic expression is bounded.
SELECT max(length(replaceRegexpOne(explain, '^([^A-Za-z]*[A-Za-z][A-Za-z ]*: )', ''))) <= 8195 FROM (EXPLAIN SELECT (c15 + c15 + c15) AS c16 FROM (SELECT (c14 + c14 + c14) AS c15 FROM (SELECT (c13 + c13 + c13) AS c14 FROM (SELECT (c12 + c12 + c12) AS c13 FROM (SELECT (c11 + c11 + c11) AS c12 FROM (SELECT (c10 + c10 + c10) AS c11 FROM (SELECT (c9 + c9 + c9) AS c10 FROM (SELECT (c8 + c8 + c8) AS c9 FROM (SELECT (c7 + c7 + c7) AS c8 FROM (SELECT (c6 + c6 + c6) AS c7 FROM (SELECT (c5 + c5 + c5) AS c6 FROM (SELECT (c4 + c4 + c4) AS c5 FROM (SELECT (c3 + c3 + c3) AS c4 FROM (SELECT (c2 + c2 + c2) AS c3 FROM (SELECT (c1 + c1 + c1) AS c2 FROM (SELECT (c0 + c0 + c0) AS c1 FROM (SELECT number AS c0 FROM numbers(1))))))))))))))))));

-- The whole rendered plan is bounded, not only its longest expression.
SELECT sum(length(explain)) < 100000 FROM (EXPLAIN SELECT (c15 + c15 + c15) AS c16 FROM (SELECT (c14 + c14 + c14) AS c15 FROM (SELECT (c13 + c13 + c13) AS c14 FROM (SELECT (c12 + c12 + c12) AS c13 FROM (SELECT (c11 + c11 + c11) AS c12 FROM (SELECT (c10 + c10 + c10) AS c11 FROM (SELECT (c9 + c9 + c9) AS c10 FROM (SELECT (c8 + c8 + c8) AS c9 FROM (SELECT (c7 + c7 + c7) AS c8 FROM (SELECT (c6 + c6 + c6) AS c7 FROM (SELECT (c5 + c5 + c5) AS c6 FROM (SELECT (c4 + c4 + c4) AS c5 FROM (SELECT (c3 + c3 + c3) AS c4 FROM (SELECT (c2 + c2 + c2) AS c3 FROM (SELECT (c1 + c1 + c1) AS c2 FROM (SELECT (c0 + c0 + c0) AS c1 FROM (SELECT number AS c0 FROM numbers(1))))))))))))))))));

-- The descent must STOP when the budget is spent, not merely have its output clipped afterwards. With
-- one budget per recursive call the rendered text is identical but the walk stays 3^N: measured 0.19s
-- at depth 12 and 110s at this depth, versus a flat 0.09s here. Deeper is not better: the planner
-- recurses once per nesting level before anything is rendered, and under TSan it may use only 5
-- percent of the stack, which a chain half again as deep exhausts on its own.
SELECT max(length(replaceRegexpOne(explain, '^([^A-Za-z]*[A-Za-z][A-Za-z ]*: )', ''))) <= 8195 FROM (EXPLAIN SELECT (c17 + c17 + c17) AS c18 FROM (SELECT (c16 + c16 + c16) AS c17 FROM (SELECT (c15 + c15 + c15) AS c16 FROM (SELECT (c14 + c14 + c14) AS c15 FROM (SELECT (c13 + c13 + c13) AS c14 FROM (SELECT (c12 + c12 + c12) AS c13 FROM (SELECT (c11 + c11 + c11) AS c12 FROM (SELECT (c10 + c10 + c10) AS c11 FROM (SELECT (c9 + c9 + c9) AS c10 FROM (SELECT (c8 + c8 + c8) AS c9 FROM (SELECT (c7 + c7 + c7) AS c8 FROM (SELECT (c6 + c6 + c6) AS c7 FROM (SELECT (c5 + c5 + c5) AS c6 FROM (SELECT (c4 + c4 + c4) AS c5 FROM (SELECT (c3 + c3 + c3) AS c4 FROM (SELECT (c2 + c2 + c2) AS c3 FROM (SELECT (c1 + c1 + c1) AS c2 FROM (SELECT (c0 + c0 + c0) AS c1 FROM (SELECT number AS c0 FROM numbers(1))))))))))))))))))));

-- Truncation is visible instead of silently dropping the tail.
SELECT count() > 0 FROM (EXPLAIN SELECT (c15 + c15 + c15) AS c16 FROM (SELECT (c14 + c14 + c14) AS c15 FROM (SELECT (c13 + c13 + c13) AS c14 FROM (SELECT (c12 + c12 + c12) AS c13 FROM (SELECT (c11 + c11 + c11) AS c12 FROM (SELECT (c10 + c10 + c10) AS c11 FROM (SELECT (c9 + c9 + c9) AS c10 FROM (SELECT (c8 + c8 + c8) AS c9 FROM (SELECT (c7 + c7 + c7) AS c8 FROM (SELECT (c6 + c6 + c6) AS c7 FROM (SELECT (c5 + c5 + c5) AS c6 FROM (SELECT (c4 + c4 + c4) AS c5 FROM (SELECT (c3 + c3 + c3) AS c4 FROM (SELECT (c2 + c2 + c2) AS c3 FROM (SELECT (c1 + c1 + c1) AS c2 FROM (SELECT (c0 + c0 + c0) AS c1 FROM (SELECT number AS c0 FROM numbers(1)))))))))))))))))) WHERE explain LIKE '%...%';

-- EXPLAIN ANALYZE renders through the same path.
SELECT max(length(replaceRegexpOne(explain, '^([^A-Za-z]*[A-Za-z][A-Za-z ]*: )', ''))) <= 8195 FROM (EXPLAIN ANALYZE SELECT (c15 + c15 + c15) AS c16 FROM (SELECT (c14 + c14 + c14) AS c15 FROM (SELECT (c13 + c13 + c13) AS c14 FROM (SELECT (c12 + c12 + c12) AS c13 FROM (SELECT (c11 + c11 + c11) AS c12 FROM (SELECT (c10 + c10 + c10) AS c11 FROM (SELECT (c9 + c9 + c9) AS c10 FROM (SELECT (c8 + c8 + c8) AS c9 FROM (SELECT (c7 + c7 + c7) AS c8 FROM (SELECT (c6 + c6 + c6) AS c7 FROM (SELECT (c5 + c5 + c5) AS c6 FROM (SELECT (c4 + c4 + c4) AS c5 FROM (SELECT (c3 + c3 + c3) AS c4 FROM (SELECT (c2 + c2 + c2) AS c3 FROM (SELECT (c1 + c1 + c1) AS c2 FROM (SELECT (c0 + c0 + c0) AS c1 FROM (SELECT number AS c0 FROM numbers(1))))))))))))))))));

DROP TABLE IF EXISTS t_05024;
CREATE TABLE t_05024 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_05024 SELECT number FROM numbers(10);

-- A source filter is split into conjunction atoms before it is rendered, and that split follows every
-- path into a shared subtree, so the atom count is exponential where the condition is not. Bounded and
-- marked, and flat in depth: identical numbers at depth 6 and depth 7 below.
SELECT max(length(replaceRegexpOne(explain, '^([^A-Za-z]*[A-Za-z][A-Za-z ]*: )', ''))) <= 8195, sum(length(explain)) < 100000, countIf(explain LIKE '%...%') > 0 FROM (EXPLAIN WITH toUInt8(number = 0) AS c0, (c0 AND c0 AND c0) AS c1, (c1 AND c1 AND c1) AS c2, (c2 AND c2 AND c2) AS c3, (c3 AND c3 AND c3) AS c4, (c4 AND c4 AND c4) AS c5, (c5 AND c5 AND c5) AS c6 SELECT number FROM t_05024 PREWHERE c6);
SELECT max(length(replaceRegexpOne(explain, '^([^A-Za-z]*[A-Za-z][A-Za-z ]*: )', ''))) <= 8195, sum(length(explain)) < 100000, countIf(explain LIKE '%...%') > 0 FROM (EXPLAIN WITH toUInt8(number = 0) AS c0, (c0 AND c0 AND c0) AS c1, (c1 AND c1 AND c1) AS c2, (c2 AND c2 AND c2) AS c3, (c3 AND c3 AND c3) AS c4, (c4 AND c4 AND c4) AS c5, (c5 AND c5 AND c5) AS c6, (c6 AND c6 AND c6) AS c7 SELECT number FROM t_05024 PREWHERE c7);

-- The walk over the conjunction is bounded too, not only the atoms it keeps: the per-class quotas are
-- a conjunction over two classes, and a query without a runtime filter never fills the second one. The
-- bound must not invent an annotation either, so this asserts the plan carries no runtime filter line:
-- a walk stopped early knows only that it has more of the classes it HAS collected.
SELECT countIf(explain LIKE '%Runtime filters%') FROM (EXPLAIN WITH toUInt8(number = 0) AS c0, (c0 AND c0 AND c0) AS c1, (c1 AND c1 AND c1) AS c2, (c2 AND c2 AND c2) AS c3, (c3 AND c3 AND c3) AS c4, (c4 AND c4 AND c4) AS c5, (c5 AND c5 AND c5) AS c6 SELECT number FROM t_05024 PREWHERE c6);

-- A multi-argument aggregate: one argument cannot expose a bug in joining several bounded arguments.
-- Marker asserted on the `Aggregates:` line itself: several labels carry one on this plan, so a
-- plan-wide test is satisfied by another emitter and would accept an unmarked aggregate list.
-- Exact length, so a list replaced by a short marked string does not pass an upper bound. It also
-- fails on an empty selection, where `max(length(...))` is 0 rather than NULL.
SELECT max(length(extract(explain, '^[^A-Za-z]*Aggregates: (.*)$'))) = 8195, sum(length(explain)) < 100000, countIf(explain LIKE '%Aggregates:%' AND explain LIKE '%...') > 0 FROM (EXPLAIN WITH toUInt8(number = 0) AS c0, (c0 AND c0 AND c0) AS c1, (c1 AND c1 AND c1) AS c2, (c2 AND c2 AND c2) AS c3, (c3 AND c3 AND c3) AS c4, (c4 AND c4 AND c4) AS c5, (c5 AND c5 AND c5) AS c6 SELECT argMaxIf(c6, c6, c6 != 0) FROM t_05024);

-- A window function's own argument list, which composes bounded names outside the render descent.
-- Exact length, so a list truncated early does not pass, and the marker must trail this same line.
SELECT max(length(extract(explain, '^[^A-Za-z]*Functions: (.*)$'))) = 8195, countIf(explain LIKE '%Functions:%' AND explain LIKE '%...') > 0 FROM (EXPLAIN WITH toUInt8(number = 0) AS c0, (c0 AND c0 AND c0) AS c1, (c1 AND c1 AND c1) AS c2, (c2 AND c2 AND c2) AS c3, (c3 AND c3 AND c3) AS c4, (c4 AND c4 AND c4) AS c5, (c5 AND c5 AND c5) AS c6 SELECT argMax(c6, c6) OVER (PARTITION BY c6 ORDER BY c6) FROM t_05024) WHERE explain LIKE '%Functions:%';

DROP TABLE t_05024;

-- One conjunction can hold both a user condition and a runtime filter, and they are rendered as two
-- separate lines, so the atom cap is counted per class: one cap over the conjunction is spent by
-- whichever class the walk reaches first and the other line is emitted with nothing in it. Reaching
-- the shipped cap needs more join keys than is reasonable here, so this arm covers the shape rather
-- than the cap: both lines must name their own content. Both prewhere settings are pinned because
-- either one off leaves the condition in a Filter step, where no runtime filter joins it.
DROP TABLE IF EXISTS trf1_05024;
DROP TABLE IF EXISTS trf2_05024;
CREATE TABLE trf1_05024 (a UInt64, k UInt64, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE trf2_05024 (x UInt64, w UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO trf1_05024 SELECT number, number, toString(number) FROM numbers(10);
INSERT INTO trf2_05024 SELECT number, number FROM numbers(10);
SELECT countIf(explain LIKE '%Prewhere filter column:%' AND explain LIKE '%b != %') > 0, countIf(explain LIKE '%Runtime filters: RF%') > 0 FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1 SELECT trf1_05024.a FROM trf1_05024 INNER JOIN trf2_05024 ON trf1_05024.a = trf2_05024.x AND trf1_05024.k = trf2_05024.w WHERE b != 'q' SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 0, query_plan_join_swap_table = 0, enable_parallel_replicas = 0, use_statistics = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_randomize = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 0);
DROP TABLE trf1_05024;
DROP TABLE trf2_05024;

-- The conjunction is split into atoms BEFORE any of it is rendered, and that split follows every path
-- into a shared subtree, so it collects one atom per path while the DAG holds a single node. Bounding
-- only the emitted text leaves that vector exponential, and it costs memory and time rather than
-- bytes, so this arm is about completing at all: 2^13 paths resolve to one distinct node here.
-- ALIAS columns keep the query itself small, so the query-tree size guard does not mask the walk.
DROP TABLE IF EXISTS ta_05024;
CREATE TABLE ta_05024 (number UInt64, c0 UInt8 ALIAS toUInt8(number = 0), c1 UInt8 ALIAS c0 AND c0, c2 UInt8 ALIAS c1 AND c1, c3 UInt8 ALIAS c2 AND c2, c4 UInt8 ALIAS c3 AND c3, c5 UInt8 ALIAS c4 AND c4, c6 UInt8 ALIAS c5 AND c5, c7 UInt8 ALIAS c6 AND c6, c8 UInt8 ALIAS c7 AND c7, c9 UInt8 ALIAS c8 AND c8, c10 UInt8 ALIAS c9 AND c9, c11 UInt8 ALIAS c10 AND c10, c12 UInt8 ALIAS c11 AND c11, c13 UInt8 ALIAS c12 AND c12) ENGINE = MergeTree ORDER BY number;
INSERT INTO ta_05024(number) SELECT number FROM numbers(10);
SELECT max(length(replaceRegexpOne(explain, '^([^A-Za-z]*[A-Za-z][A-Za-z ]*: )', ''))) <= 8195, countIf(explain LIKE '%...%') > 0 FROM (EXPLAIN SELECT number FROM ta_05024 PREWHERE c13);
DROP TABLE ta_05024;

-- Control: an ordinary expression is printed in full.
SELECT count() > 0 FROM (EXPLAIN SELECT number + 1 AS x FROM numbers(1)) WHERE explain LIKE '%number + 1%';

-- Control: a wide but realistic expression, longer than any in the existing test references, is
-- printed whole. These two arms fail if the bound is low enough to truncate legitimate output.
SELECT count() FROM (EXPLAIN SELECT number + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19 + 20 + 21 + 22 + 23 + 24 + 25 + 26 + 27 + 28 + 29 + 30 + 31 + 32 + 33 + 34 + 35 + 36 + 37 + 38 + 39 + 40 AS y FROM numbers(1)) WHERE explain LIKE '%...%';
SELECT count() > 0 FROM (EXPLAIN SELECT number + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19 + 20 + 21 + 22 + 23 + 24 + 25 + 26 + 27 + 28 + 29 + 30 + 31 + 32 + 33 + 34 + 35 + 36 + 37 + 38 + 39 + 40 AS y FROM numbers(1)) WHERE explain LIKE '%+ 39 + 40%';

-- Control: an ordinary aggregate and an ordinary window function print every argument, unmarked.
SELECT count() > 0 FROM (EXPLAIN SELECT sum(number), count() FROM numbers(10)) WHERE explain LIKE '%sum(number)%' AND explain NOT LIKE '%...%';
SELECT count() > 0 FROM (EXPLAIN SELECT sum(number) OVER (PARTITION BY number ORDER BY number) FROM numbers(10)) WHERE explain LIKE '%sum(number) OVER (PARTITION BY number ORDER BY number ASC)%';
