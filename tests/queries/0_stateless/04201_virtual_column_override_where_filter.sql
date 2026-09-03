-- Test: exercises `MergeTreeData::getHeaderWithVirtualsForFilter` when a user column overrides the
-- `_part` virtual column AND a WHERE filter is applied on that column.
-- Covers: src/Storages/MergeTree/MergeTreeData.cpp:1661 — `if (columns.contains(name)) continue;`
--         exercised via filterPartsByVirtualColumns with a non-null predicate
--         (different code path from PR's own simple SELECT and GROUP BY tests).

DROP TABLE IF EXISTS override_where_filter_test;
CREATE TABLE override_where_filter_test (_part UInt32, x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO override_where_filter_test VALUES (1, 100), (2, 200), (3, 300);

SELECT _part, x FROM override_where_filter_test WHERE _part = 2 ORDER BY _part;
SELECT count() FROM override_where_filter_test WHERE _part = 2;
SELECT count() FROM override_where_filter_test WHERE _part IN (1, 3);
SELECT sum(x) FROM override_where_filter_test WHERE _part >= 2;

DROP TABLE override_where_filter_test;
