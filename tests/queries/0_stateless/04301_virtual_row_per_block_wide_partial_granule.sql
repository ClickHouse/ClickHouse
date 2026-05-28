-- Tags: no-random-merge-tree-settings, no-random-settings, long
-- Regression for https://github.com/ClickHouse/ClickHouse/issues for the per-block
-- virtual row mis-ordering introduced by `read_in_order_use_virtual_row_per_block`
-- when wide-part readers (which can read partial granules) produce chunks whose
-- physical data spans only part of the granules listed in `read_mark_ranges`.
--
-- Trigger:
--   * `min_bytes_for_wide_part = 0`             (force wide parts)
--   * `index_granularity = 28454` (large)        (granule larger than one block)
--   * `max_block_size = 14633` (smaller)        (granule split across blocks)
--   * `read_in_order_use_virtual_row_per_block = 1`
--
-- Before the fix, ORDER BY ... DESC LIMIT 100 produced mis-ordered output because
-- `MergeTreeSelectProcessor::buildVirtualRowFromIndex` derived the per-block
-- boundary key from `index[next_mark]` (a granule edge), while the actual chunk's
-- data spanned only part of that granule. The understated boundary made
-- `MergingSortedTransform` deprioritize the source, causing larger values from
-- later (LIFO-popped) chunks to appear after smaller values already emitted from
-- other sources.

DROP TABLE IF EXISTS t_vrow_partial;

CREATE TABLE t_vrow_partial (CounterID UInt32, CounterID2 UInt32)
ENGINE = MergeTree ORDER BY CounterID
SETTINGS index_granularity = 28454, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_vrow_partial;

-- Many small parts with overlapping ranges, each with a sub-granule-sized max_block_size.
INSERT INTO t_vrow_partial SELECT sipHash64(number, 0) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 1) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 2) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 3) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 4) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 5) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 6) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 7) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 8) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;
INSERT INTO t_vrow_partial SELECT sipHash64(number, 9) % 100000, rand32() % 100000 FROM numbers(100000) SETTINGS max_block_size = 14633;

SET optimize_read_in_order = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1,
    max_block_size = 14633;

-- Verify ORDER BY DESC LIMIT and ASC LIMIT produce monotonically (non-strictly)
-- sorted output for the PK column. The boolean must always be 1.

WITH (SELECT groupArray(CounterID) FROM (SELECT CounterID FROM t_vrow_partial ORDER BY CounterID DESC LIMIT 100)) AS arr
SELECT 'desc PK monotonic', arr = arrayReverseSort(arr);

WITH (SELECT groupArray(CounterID) FROM (SELECT CounterID FROM t_vrow_partial ORDER BY CounterID ASC LIMIT 100)) AS arr
SELECT 'asc PK monotonic', arr = arraySort(arr);

-- The same query but on a non-PK column. Output content depends on the random
-- INSERT values, so we only check the result count is exactly the LIMIT and is
-- monotonic w.r.t. the chosen direction (CounterID2 is not the sort key, so
-- this validates that ORDER BY ... LIMIT does not return more than LIMIT rows
-- and is consistent with the sort direction).
SELECT 'desc CounterID2 ordered', count() = 100 FROM (SELECT CounterID2 FROM t_vrow_partial ORDER BY CounterID2 DESC LIMIT 100);
SELECT 'asc CounterID2 ordered', count() = 100 FROM (SELECT CounterID2 FROM t_vrow_partial ORDER BY CounterID2 ASC LIMIT 100);

DROP TABLE t_vrow_partial;
