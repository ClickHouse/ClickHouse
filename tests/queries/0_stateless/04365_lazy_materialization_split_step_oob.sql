-- Tags: no-old-analyzer
-- no-old-analyzer: lazy materialization is gated on the analyzer, so the EXPLAIN assertion
-- below (JoinLazyColumnsStep / LazilyReadFromMergeTree) is empty under the old analyzer; that
-- CI variant also forbids changing enable_analyzer inside a subquery.
--
-- Regression test for issue #101567: OOB in splitExpressionStep and splitFilterStep during
-- optimizeLazyMaterialization2. The first query covers splitExpressionStep over a projection;
-- it absorbs its predicate into the projection PREWHERE, so no FilterStep survives to split.
-- The second query keeps its filter, so it additionally covers splitFilterStep. Both select
-- several columns, so the split runs with more than one required output, which is the shape
-- the issue flagged. The EXPLAIN assertions fail if the lazy split path is not produced, so
-- neither query can pass as a no-op.

DROP TABLE IF EXISTS test_split_oob4;
CREATE TABLE test_split_oob4
(
    id UInt64, url String, region String,
    extra1 String DEFAULT 'e1', extra2 String DEFAULT 'e2',
    PROJECTION region_proj (SELECT _part_offset, extra1, extra2 ORDER BY region, url)
)
ENGINE = MergeTree ORDER BY (id)
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO test_split_oob4 (id, url, region) VALUES (1, 'page1', 'europe');
INSERT INTO test_split_oob4 (id, url, region) VALUES (2, 'page2', 'us_west');

OPTIMIZE TABLE test_split_oob4 FINAL;

-- splitExpressionStep half.
-- Plan check: the lazy-materialization split path over the projection must be present.
-- query_plan_max_limit_for_lazy_materialization must be >= the LIMIT or the optimization
-- is skipped (the CI randomizer can set it to 1, which would drop the lazy steps).
-- optimize_use_projections is randomized off, and it also neuters force_optimize_projection.
-- pretty = 0: the pretty format prefixes step names, which would defeat the exact match.
SELECT trimLeft(explain) AS s FROM (
    EXPLAIN actions = 0, pretty = 0
    SELECT url, extra1, extra2 FROM test_split_oob4 WHERE region = 'europe' ORDER BY url LIMIT 10
    SETTINGS query_plan_remove_unused_columns = 0, enable_multiple_prewhere_read_steps = 0,
        optimize_use_projections = 1,
        force_optimize_projection = 1, force_optimize_projection_name = 'region_proj',
        query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 1000
) WHERE s IN ('JoinLazyColumnsStep', 'LazilyReadFromMergeTree', 'ReadFromMergeTree (region_proj)') ORDER BY s;

-- Correctness (and no abort under debug / ASan).
SELECT url, extra1, extra2 FROM test_split_oob4 WHERE region = 'europe' ORDER BY url LIMIT 10
SETTINGS query_plan_remove_unused_columns = 0, enable_multiple_prewhere_read_steps = 0,
    optimize_use_projections = 1,
    force_optimize_projection = 1, force_optimize_projection_name = 'region_proj',
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE test_split_oob4;

-- splitFilterStep half: a FilterStep only survives into the split region when the predicate
-- cannot be moved to PREWHERE, so both prewhere optimizations are disabled here. There are
-- two independent ones and either being off is enough to keep the filter, so pin both.
DROP TABLE IF EXISTS test_split_oob4_filter;
CREATE TABLE test_split_oob4_filter (id UInt64, a String, b String, c String)
ENGINE = MergeTree ORDER BY (id)
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO test_split_oob4_filter
SELECT number, 'a' || toString(number), 'b' || toString(number), 'c' || toString(number)
FROM numbers(20);

SELECT trimLeft(explain) AS s FROM (
    EXPLAIN actions = 0, pretty = 0
    SELECT a, b, c FROM test_split_oob4_filter WHERE id > 2 ORDER BY a LIMIT 5
    SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
        query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 1000
) WHERE s IN ('JoinLazyColumnsStep', 'LazilyReadFromMergeTree', 'Filter') ORDER BY s;

SELECT a, b, c FROM test_split_oob4_filter WHERE id > 2 ORDER BY a LIMIT 5
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE test_split_oob4_filter;
