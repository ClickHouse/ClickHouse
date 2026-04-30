-- Verify that projection selection chooses the right projection when
-- it has a much smaller index_granularity than the parent table.

CREATE TABLE t
(
    x UInt32,
    y UInt32,
    PROJECTION prj_y (SELECT x, y ORDER BY y) WITH SETTINGS (index_granularity = 8)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 8192;

INSERT INTO t SELECT number, number % 16 FROM numbers(10000);

-- The projection should be chosen: filtering by y on the projection reads far fewer
-- rows despite having more marks due to the smaller granularity.
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM t WHERE y = 0)
WHERE explain LIKE '%ReadFromMergeTree%';
