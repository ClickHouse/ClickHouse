SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_merge_expression_into_join = 1;
SET enable_join_runtime_filters = 0;

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

-- Results must be unaffected by the orientation change.
SELECT 'rows', count() FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = 'nowhere') AS d
    JOIN fact_04726 ON d.id = fact_04726.id;

SELECT 'rows non-empty filter', count() FROM (SELECT * FROM dim_04726 JOIN nation_04726 USING (nation_id) WHERE name = '2') AS d
    JOIN fact_04726 ON d.id = fact_04726.id;

DROP TABLE fact_04726;
DROP TABLE dim_04726;
DROP TABLE nation_04726;
