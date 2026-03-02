-- Tags: no-random-merge-tree-settings

-- Aggregate projections with constant GROUP BY keys produce 1 row from 0 input rows.
-- When TTL deletes all rows during merge, the input block has 0 rows.
-- The projection must handle this gracefully instead of throwing a LOGICAL_ERROR.

DROP TABLE IF EXISTS t_proj_ttl;

CREATE TABLE t_proj_ttl
(
    c0 DateTime64(5) MATERIALIZED nowInBlock(),
    c1 Int32
)
ENGINE = MergeTree
ORDER BY tuple(c0, sipHash128(c1))
TTL c0 + toIntervalMillisecond(2)
SETTINGS index_granularity = 4096;

ALTER TABLE t_proj_ttl ADD PROJECTION p1 (SELECT NULL GROUP BY 0.674);

-- Insert several parts so that a merge will be triggered
INSERT INTO t_proj_ttl (c1) SELECT number FROM numbers(100);
INSERT INTO t_proj_ttl (c1) SELECT number FROM numbers(100);
INSERT INTO t_proj_ttl (c1) SELECT number FROM numbers(100);

-- Wait for the TTL to expire
SELECT sleepEachRow(0.1) FROM numbers(10) FORMAT Null;

-- Force a merge which will rebuild the projection on TTL-deleted (empty) blocks
OPTIMIZE TABLE t_proj_ttl FINAL;

SELECT 'OK';

DROP TABLE t_proj_ttl;
