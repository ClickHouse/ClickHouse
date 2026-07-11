-- Tags: no-random-merge-tree-settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99388
-- CLEAR COLUMN must rebuild projections that depend on the cleared column,
-- otherwise stale projection data causes sort order violations during merge.

SET mutations_sync = 2;

-- Test compact parts
DROP TABLE IF EXISTS test_compact;

CREATE TABLE test_compact
(
    c0 Int,
    c1 Int,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';

INSERT INTO test_compact SELECT number, number FROM numbers(10);
ALTER TABLE test_compact CLEAR COLUMN c1;

-- After clearing c1, all values should be 0 (default), so count where c1 == 0 should be 10.
SELECT count() FROM test_compact WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT count() FROM test_compact WHERE c1 = 0 SETTINGS optimize_use_projections = 0;

DROP TABLE test_compact;

-- Test wide parts
DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_wide
(
    c0 Int,
    c1 Int,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_wide SELECT number, number FROM numbers(10);
ALTER TABLE test_wide CLEAR COLUMN c1;

-- After clearing c1, all values should be 0 (default), so count where c1 == 0 should be 10.
SELECT count() FROM test_wide WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT count() FROM test_wide WHERE c1 = 0 SETTINGS optimize_use_projections = 0;

DROP TABLE test_wide;
