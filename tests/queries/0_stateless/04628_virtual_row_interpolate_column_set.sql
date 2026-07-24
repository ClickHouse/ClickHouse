-- https://github.com/ClickHouse/ClickHouse/issues/111831
-- The virtual row (`read_in_order_use_virtual_row`) must not materialize the
-- `IN`-set's `ColumnSet` placeholder that `INTERPOLATE` keeps alive in the
-- in-order read header: building a default-valued constant for it threw
-- `Cannot insert element into Set` from `MergingSortedTransform`.

DROP TABLE IF EXISTS t_virtual_row_set;
CREATE TABLE t_virtual_row_set (k UInt32, s String) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_virtual_row_set;
-- two parts: the virtual row is the per-part boundary marker
INSERT INTO t_virtual_row_set VALUES (1, '8');
INSERT INTO t_virtual_row_set VALUES (2, '8');

SELECT k, s FROM t_virtual_row_set WHERE s IN ('8')
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS read_in_order_use_virtual_row = 1;

DROP TABLE t_virtual_row_set;
