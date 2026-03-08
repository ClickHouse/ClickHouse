-- Regression test for FutureSet identity mismatch in TTLDeleteFilterStep.
-- When vertical merge is used with TTL ... WHERE col IN (SELECT ...),
-- the FutureSet objects must be shared between CreatingSetStep and the
-- TTLDeleteFilterTransform. Previously, transformPipeline() created new
-- transforms with new FutureSet objects that were never filled.

DROP TABLE IF EXISTS t_ttl_vertical;

CREATE TABLE t_ttl_vertical
(
    a UInt32,
    timestamp DateTime
)
ENGINE = MergeTree
ORDER BY a
TTL timestamp + INTERVAL 1 SECOND WHERE a IN (SELECT number FROM system.numbers LIMIT 10)
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

-- Insert two parts so OPTIMIZE FINAL triggers a merge.
INSERT INTO t_ttl_vertical SELECT number, now() - 5 FROM numbers(100);
INSERT INTO t_ttl_vertical SELECT number + 100, now() - 5 FROM numbers(100);

OPTIMIZE TABLE t_ttl_vertical FINAL;

SELECT count() FROM t_ttl_vertical;

DROP TABLE t_ttl_vertical;
