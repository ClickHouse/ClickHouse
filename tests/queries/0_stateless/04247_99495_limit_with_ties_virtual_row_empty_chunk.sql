-- Tags: no-parallel-replicas
-- Regression test for ClickHouse/ClickHouse#99495:
-- `LIMIT ... WITH TIES` aborted with `Assertion 'row < chunk.getNumRows()' failed` in
-- `LimitTransform::makeChunkWithPreviousRow` (`src/Processors/LimitTransform.cpp:60`) when the
-- read-in-order pipeline produced a per-block virtual-row chunk that reached `LimitTransform`
-- after `rows_read` had already hit the limit boundary.
--
-- The virtual-row chunk is empty (zero rows) but carries `MergeTreeReadInfo`; when it arrived
-- with `rows_read == offset + limit` and `with_ties`, `makeChunkWithPreviousRow` was called with
-- `chunk.getNumRows() - 1`, which underflows to `UINT64_MAX` and trips the assertion.

DROP TABLE IF EXISTS t_99495;

CREATE TABLE t_99495
(
    id UInt64,
    m Map(String, String),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_99495 VALUES (1, {'1': '1'});
INSERT INTO t_99495 VALUES (2, {'2': '2'});
INSERT INTO t_99495 VALUES (3, {'3': '3'});

-- Pre-fix: aborts the server in debug builds, throws in release builds.
-- Post-fix: returns the single matching row.
SELECT id
FROM t_99495
PREWHERE mapContains(m, toFixedString('1', 1))
ORDER BY id ASC
LIMIT 1 WITH TIES
SETTINGS
    optimize_read_in_order = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1;

-- Same query but using OFFSET: another path that previously hit the same assertion.
SELECT id
FROM t_99495
PREWHERE mapContains(m, toFixedString('1', 1))
ORDER BY id ASC
LIMIT 0, 1 WITH TIES
SETTINGS
    optimize_read_in_order = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1;

DROP TABLE t_99495;
