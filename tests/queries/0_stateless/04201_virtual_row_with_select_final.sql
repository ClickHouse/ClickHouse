-- Tags: no-random-merge-tree-settings
-- Test: exercises FINAL-disable guard for virtual row optimization at
-- src/Processors/QueryPlan/ReadFromMergeTree.cpp:2896 (introduced by PR #62125).
-- The guard (`if (isQueryWithFinal() || ...) return false;`) prevents virtual rows
-- from being inserted into the pipeline for `SELECT ... FROM table FINAL` queries.
-- If removed, virtual rows would flow through `ReplacingSorted` (or other final
-- merging algorithms) which do not skip them, producing extra/wrong rows.

DROP TABLE IF EXISTS t_vrow_select_final;

CREATE TABLE t_vrow_select_final (x UInt64, y UInt64)
ENGINE = ReplacingMergeTree(y)
ORDER BY x;

SYSTEM STOP MERGES t_vrow_select_final;

-- Two parts with overlapping `x` so FINAL must dedup. Both parts produce a virtual
-- row (the first PK value of each part) when virtual_row optimization triggers.
INSERT INTO t_vrow_select_final SELECT number, 1 FROM numbers(8192);
INSERT INTO t_vrow_select_final SELECT number, 2 FROM numbers(8192);

-- With virtual_row=1: FINAL guard returns false from setVirtualRowConversions,
-- so no `VirtualRowTransform` is added; ReplacingSorted dedups normally.
SELECT count(), sum(y)
FROM (SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1);

-- With virtual_row=0: same baseline result.
SELECT count(), sum(y)
FROM (SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 0);

-- Without FINAL the optimization stays active; produces all 16384 rows.
SELECT count()
FROM (SELECT x FROM t_vrow_select_final ORDER BY x
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1);

DROP TABLE t_vrow_select_final;
