SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_merge_expression_into_join = 1;
SET enable_join_runtime_filters = 0;
-- The hash-table cache is process-global and carries counts from earlier runs, so leaving this
-- randomized would let a previous run of this test supply the estimate the arms below withhold.
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS fact_04726;
DROP TABLE IF EXISTS dim_04726;
DROP TABLE IF EXISTS nation_04726;

-- `auto_statistics_types = ''` keeps the relations without column statistics regardless of
-- server defaults, so the join order optimizer takes the primary-index estimate path.
CREATE TABLE fact_04726 (id Int32, val Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
CREATE TABLE dim_04726 (id Int32, nation_id Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
CREATE TABLE nation_04726 (nation_id Int32, name String) ENGINE = MergeTree ORDER BY nation_id
    SETTINGS auto_statistics_types = '';

INSERT INTO fact_04726 SELECT number, number FROM numbers(100000);
INSERT INTO dim_04726 SELECT number, number % 4 + 5 FROM numbers(100);
INSERT INTO nation_04726 SELECT number + 5, toString(number) FROM numbers(4);

-- `name` is not in any index, so the filtered relation gets no row estimate at all. The join
-- reordering must still keep the small filtered side on the hash-join build side (the right
-- side of the emitted join), using the relation's row-count upper bound. Asserting the plan
-- rather than a timing keeps the test deterministic.

SELECT 'greedy', count() > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT avg(val)
    FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
    JOIN fact_04726 ON d.id = fact_04726.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy'
) WHERE explain ILIKE '%Join: fact_04726%';

SELECT 'dpsub', count() > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT avg(val)
    FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
    JOIN fact_04726 ON d.id = fact_04726.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub'
) WHERE explain ILIKE '%Join: fact_04726%';

SELECT 'dpsize', count() > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT avg(val)
    FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
    JOIN fact_04726 ON d.id = fact_04726.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain ILIKE '%Join: fact_04726%';

SELECT 'dphyp', count() > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT avg(val)
    FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
    JOIN fact_04726 ON d.id = fact_04726.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy'
) WHERE explain ILIKE '%Join: fact_04726%';

-- The bound is only trusted against a right side whose estimate was actually measured, so an
-- unmeasured right side must not trigger the fallback. A stat hint is the one source that is
-- reachable from SQL and rejected by design, which makes it the observable for that guard.
SET param__internal_join_table_stat_hints = '{"fact_04726": {"cardinality": 999999999}}';

SELECT 'unmeasured right side keeps orientation', count() > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM dim_04726 JOIN fact_04726 ON dim_04726.id = fact_04726.id
    WHERE dim_04726.nation_id = 5
) WHERE explain ILIKE '%Join: dim_04726%';

SET param__internal_join_table_stat_hints = '{}';

-- A right side read through the primary index is only an upper bound: `id < 10000` selects whole
-- granules while `pad = 7` then drops nearly all of them, so its estimate exceeds its true row
-- count by far. Comparing the left bound against such an estimate must not flip the join, or the
-- bigger side would land on the build side. The range is wide enough that the estimate stays above
-- the left bound of 400 at every `index_granularity` the runner randomizes, so a build that wrongly
-- trusted it would always swap here.
CREATE TABLE residual_04726 (id Int32, pad Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO residual_04726 SELECT number, number % 100000 FROM numbers(100000);

-- `residual_04726` must never become the first input of a join, i.e. it stays on the build side.
-- The first condition keeps the assertion non-vacuous if the relation stops appearing at all; the
-- second is the property itself, over every join of the plan rather than a fixed number of them,
-- so a different join order does not change the verdict.
SELECT 'overstated right side keeps orientation',
        countIf(explain ILIKE '%residual\_04726%') > 0
    AND countIf(explain ILIKE '%Join: residual\_04726%') = 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count()
    FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = '2') AS d
    JOIN residual_04726 ON d.id = residual_04726.id
    WHERE residual_04726.id < 10000 AND residual_04726.pad = 7
) WHERE explain ILIKE '%Join:%';

DROP TABLE residual_04726;

-- A lightweight delete removes rows only while reading, so the range row count is above the true
-- one and must not count as a lower bound. `deleted_04726` holds one live row out of 100000, so a
-- plan that trusts the range count puts the bigger side on the build side.
CREATE TABLE deleted_04726 (id Int32, pad Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO deleted_04726 SELECT number, number % 7 FROM numbers(100000);
DELETE FROM deleted_04726 WHERE id > 0;

CREATE TABLE small_04726 (id Int32, pad Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO small_04726 SELECT number, number % 7 FROM numbers(20);

SELECT 'deleted right side keeps orientation',
        countIf(explain ILIKE '%deleted\_04726%') > 0
    AND countIf(explain ILIKE '%Join: deleted\_04726%') = 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM small_04726 JOIN deleted_04726 ON small_04726.id = deleted_04726.id
    WHERE small_04726.pad = 3
) WHERE explain ILIKE '%Join:%';

DROP TABLE deleted_04726;
DROP TABLE small_04726;

-- A relation of exactly one row bounds its own row count from above too, and a composite bound is
-- the product of both children's bounds, so a one-row leaf carrying none erases the bound of every
-- join above it. `system.one` is what backs constant subqueries such as `(SELECT 1)`.
-- The first condition keeps the assertion non-vacuous: were the one-row leaf dropped from the join
-- graph, the query would degenerate into the two-relation case that already passes without it.
SELECT 'one-row leaf preserves the composite bound',
        countIf(explain ILIKE '%system.one%') > 0
    AND countIf(explain ILIKE '%Join: fact\_04726%') > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM (
        SELECT dim_04726.id AS id FROM dim_04726 JOIN system.one ON 1 WHERE dim_04726.nation_id = 5
    ) AS d JOIN fact_04726 ON d.id = fact_04726.id
) WHERE explain ILIKE '%Join:%';

-- The plan arms above assert the orientation; this one asserts the effect it exists for, so a
-- future change cannot keep the plan shape while losing the small build side at runtime.
SELECT avg(val)
FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
JOIN fact_04726 ON d.id = fact_04726.id
SETTINGS log_comment = '04726_build_side_profile_events' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The filter matches nothing, so the build side is empty and the empty-build short circuit must
-- keep the fact table unread. Asserting `SelectedRows` too means a plan that keeps the asserted
-- shape while still scanning the fact table cannot pass.
SELECT 'build side rows', ProfileEvents['JoinBuildTableRowCount'], ProfileEvents['SelectedRows']
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase() AND log_comment = '04726_build_side_profile_events'
ORDER BY event_time DESC LIMIT 1;

-- Results must be unaffected by the orientation change.
SELECT 'rows', count() FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
    JOIN fact_04726 ON d.id = fact_04726.id;

SELECT 'rows non-empty filter', count() FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = '2') AS d
    JOIN fact_04726 ON d.id = fact_04726.id;

DROP TABLE fact_04726;
DROP TABLE dim_04726;
DROP TABLE nation_04726;
