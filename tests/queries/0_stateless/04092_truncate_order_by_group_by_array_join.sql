-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101402
-- TruncateOrderByAfterGroupByKeysPass must not truncate ORDER BY when the
-- projection contains arrayJoin, because it multiplies rows after GROUP BY
-- and the "one row per group" assumption no longer holds.

SET enable_analyzer = 1;
SET optimize_truncate_order_by_after_group_by_keys = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a String, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t VALUES ('x', 3), ('x', 1), ('x', 2), ('y', 6), ('y', 4), ('y', 5);

-- Without arrayJoin the optimization is fine: one row per group.
SELECT a, sum(b) FROM t GROUP BY a ORDER BY a, sum(b);

-- With arrayJoin in the projection, rows are multiplied.
-- ORDER BY a, expanded_b must NOT be truncated to ORDER BY a.
SELECT a, arrayJoin(arraySort(groupArray(b))) AS expanded_b
FROM t
GROUP BY a
ORDER BY a, expanded_b;

-- Verify via EXPLAIN QUERY TREE that both SORT nodes are preserved
-- when arrayJoin is in the projection (should be 2 sort nodes).
SELECT count()
FROM (
    EXPLAIN QUERY TREE
    SELECT a, arrayJoin(arraySort(groupArray(b))) AS expanded_b
    FROM t
    GROUP BY a
    ORDER BY a, expanded_b
)
WHERE explain LIKE '%SORT id:%';

DROP TABLE t;
