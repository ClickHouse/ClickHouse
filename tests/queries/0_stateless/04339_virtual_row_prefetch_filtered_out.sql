-- Tags: no-random-merge-tree-settings, no-random-settings

-- Regression test for the read-ahead window used with `read_in_order_use_virtual_row`.
-- Sources deferred behind a virtual row are prefetched within a bounded window. When a
-- prefetched source is completely filtered out upstream it finishes without ever delivering
-- a real chunk, so `consume` is never called for it and the merge must still release its
-- read-ahead slot (see `MergingSortedAlgorithm::onSourceExhausted`). Otherwise the window
-- stops refilling after `max_threads` such sources and the merge silently degrades to
-- reading one part at a time. Here the leading parts the merge has to walk through are all
-- filtered out, exercising that path; the query must stay correct.

DROP TABLE IF EXISTS t_vrow_filtered;

CREATE TABLE t_vrow_filtered (a UInt64, b UInt8) ENGINE = MergeTree ORDER BY a;

SYSTEM STOP MERGES t_vrow_filtered;

-- 8 parts with disjoint, increasing key ranges, so the merge needs them strictly in part
-- order. The first 6 parts (well past `max_threads = 2`) are fully filtered out by
-- `WHERE b = 1`, so each is reached while still prefetched and finishes without data.
INSERT INTO t_vrow_filtered SELECT number + 0 * 100000, 0 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 1 * 100000, 0 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 2 * 100000, 0 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 3 * 100000, 0 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 4 * 100000, 0 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 5 * 100000, 0 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 6 * 100000, 1 FROM numbers(100000);
INSERT INTO t_vrow_filtered SELECT number + 7 * 100000, 1 FROM numbers(100000);

SELECT a FROM t_vrow_filtered WHERE b = 1 ORDER BY a LIMIT 5
SETTINGS read_in_order_use_virtual_row = 1, optimize_move_to_prewhere = 0, max_threads = 2;

DROP TABLE t_vrow_filtered;
