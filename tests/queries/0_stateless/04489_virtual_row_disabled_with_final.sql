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

DROP TABLE t_vrow_select_final;
