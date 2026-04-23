-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- no-random-*-settings: the test depends on the granule-level index being evaluated eagerly
--                       (not on data read) and on the exact per-part layout we set up.
-- no-parallel-replicas: behaviour is observed on the coordinator.

-- Verifies that when a part-level minmax index (over partition-key columns) already
-- proves a filter is true for an entire part, the granule-level minmax skip index can
-- be bypassed without changing results. This test asserts correctness across a few
-- conditions and a no-skip-indexes reference; the bypass itself is covered by the
-- optimization inside `filterPartsByPrimaryKeyAndSkipIndexes`.

DROP TABLE IF EXISTS t_skip_granule_minmax;

CREATE TABLE t_skip_granule_minmax
(
    c Int32,
    INDEX i c TYPE minmax
)
ENGINE = MergeTree()
ORDER BY tuple()
PARTITION BY indexHint(c)
SETTINGS index_granularity = 1;

INSERT INTO t_skip_granule_minmax
SELECT 123 + number FROM numbers(1000);

-- Condition fully covered by part-level minmax -> the bypass fires.
SELECT count() FROM t_skip_granule_minmax WHERE c > 100;

-- Condition only partially covered -> granule-level evaluation still runs.
SELECT count() FROM t_skip_granule_minmax WHERE c > 500;

-- Condition that prunes the part entirely -> part-level minmax returns "definitely false".
SELECT count() FROM t_skip_granule_minmax WHERE c > 10000;

-- Reference: same condition with skip indexes disabled.
SELECT count() FROM t_skip_granule_minmax WHERE c > 100 SETTINGS use_skip_indexes = 0;

-- Reference: same condition with skip indexes disabled in data-read mode.
SELECT count() FROM t_skip_granule_minmax WHERE c > 100 SETTINGS use_skip_indexes_on_data_read = 0;

DROP TABLE t_skip_granule_minmax;
