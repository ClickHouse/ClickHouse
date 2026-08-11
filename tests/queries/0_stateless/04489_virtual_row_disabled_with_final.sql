-- Tags: no-random-merge-tree-settings

-- SELECT ... FINAL must not lose rows when the read-in-order virtual row is enabled.
-- ORDER BY with a LIMIT is required: without it the read-in-order plan is not built at all.

DROP TABLE IF EXISTS t_vrow_select_final;

CREATE TABLE t_vrow_select_final (x UInt64, y UInt64)
ENGINE = ReplacingMergeTree(y)
ORDER BY x;

SYSTEM STOP MERGES t_vrow_select_final;

INSERT INTO t_vrow_select_final SELECT number, 1 FROM numbers(8192);
INSERT INTO t_vrow_select_final SELECT number, 2 FROM numbers(8192);

SELECT count(), sum(y)
FROM (SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x LIMIT 20000
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1);

SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x LIMIT 3
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;

SELECT count()
FROM (SELECT x FROM t_vrow_select_final ORDER BY x LIMIT 20000
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1);

-- The row counts above stay correct whenever the virtual row is never attempted, so assert as a
-- pair that the in-order FINAL read carries no virtual row conversion while the same fixture
-- without FINAL does build one. `ReadType:` is matched whitespace-insensitively: the label prints
-- as `Read type: ` or `ReadType: ` depending on `pretty`. The settings belong on the EXPLAIN
-- statement, not the inner subquery, so they win over session defaults; parallel replicas replace
-- the local read with `ReadFromRemoteParallelReplicas`, which carries neither line.
SELECT countIf(replaceAll(explain, ' ', '') ILIKE '%ReadType:InOrder%') > 0
   AND countIf(explain ILIKE '%Virtual row conversions%') = 0
FROM (EXPLAIN actions = 1
      SELECT count(), sum(y) FROM (SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x LIMIT 20000)
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0);

SELECT countIf(replaceAll(explain, ' ', '') ILIKE '%ReadType:InOrder%') > 0
   AND countIf(explain ILIKE '%Virtual row conversions%') > 0
FROM (EXPLAIN actions = 1
      SELECT count() FROM (SELECT x FROM t_vrow_select_final ORDER BY x LIMIT 20000)
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0);

DROP TABLE t_vrow_select_final;
