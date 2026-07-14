-- https://github.com/ClickHouse/ClickHouse/issues/91958
-- ColumnConst produced by skip index expression should not be aggregated in CoalescingMergeTree.
CREATE TABLE t0 (c0 Int, INDEX i0 ifNotFinite(1, c0) TYPE bloom_filter) ENGINE = CoalescingMergeTree ORDER BY c0;
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT * FROM t0;
DROP TABLE t0;
