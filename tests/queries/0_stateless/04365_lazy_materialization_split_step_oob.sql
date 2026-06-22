-- Tags: no-old-analyzer
-- no-old-analyzer: lazy materialization is gated on the new analyzer, so the EXPLAIN
-- assertion below (JoinLazyColumnsStep / LazilyReadFromMergeTree) is empty under the old
-- one; the old-analyzer variant also forbids changing enable_analyzer inside a subquery.
--
-- Regression test for issue #101567: OOB in splitExpressionStep / splitFilterStep during
-- optimizeLazyMaterialization2. A multi-column SELECT (url, extra1, extra2) over a projection
-- with PREWHERE routes through the split* functions with more than one required output, which
-- is the shape the issue flagged. The EXPLAIN assertion fails if the lazy split path is not
-- produced, so the test cannot pass as a no-op.

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

-- Plan check: the lazy-materialization split path must be present.
-- query_plan_max_limit_for_lazy_materialization must be >= the LIMIT or the optimization
-- is skipped (the CI randomizer can set it to 1, which would drop the lazy steps).
SELECT trimLeft(explain) AS s FROM (
    EXPLAIN actions = 0
    SELECT url, extra1, extra2 FROM test_split_oob4 WHERE region = 'europe' ORDER BY url LIMIT 10
    SETTINGS query_plan_remove_unused_columns = 0, enable_multiple_prewhere_read_steps = 0,
        force_optimize_projection = 1, force_optimize_projection_name = 'region_proj',
        query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 1000
) WHERE s IN ('JoinLazyColumnsStep', 'LazilyReadFromMergeTree') ORDER BY s;

-- Correctness (and no crash under debug / ASan).
SELECT url, extra1, extra2 FROM test_split_oob4 WHERE region = 'europe' ORDER BY url LIMIT 10
SETTINGS query_plan_remove_unused_columns = 0, enable_multiple_prewhere_read_steps = 0,
    force_optimize_projection = 1, force_optimize_projection_name = 'region_proj',
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE test_split_oob4;
