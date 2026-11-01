-- An expression added to the sorting key by ALTER can use only the columns added by the same ALTER.
-- A column retyped or renamed by it keeps its rows, and so do its subcolumns.

DROP TABLE IF EXISTS t_modify_order_by;
CREATE TABLE t_modify_order_by (k UInt32, s String, arr String, n UInt32, tp Tuple(a UInt32, b UInt32))
ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_modify_order_by MODIFY COLUMN s Tuple(a UInt32, b UInt32), MODIFY ORDER BY (k, s.a); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_order_by MODIFY COLUMN arr Array(UInt32), MODIFY ORDER BY (k, arr.size0); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_order_by RENAME COLUMN n TO m, MODIFY ORDER BY (k, m); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_order_by MODIFY ORDER BY (k, n), RENAME COLUMN n TO m; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_order_by RENAME COLUMN tp TO tp2, MODIFY ORDER BY (k, tp2.a); -- { serverError BAD_ARGUMENTS }

-- A subcolumn of a column added by the same ALTER is accepted, and so is a rename of a column the key does not use.
ALTER TABLE t_modify_order_by ADD COLUMN added Tuple(a UInt32, b UInt32), RENAME COLUMN n TO m, MODIFY ORDER BY (k, added.a);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_order_by';

DROP TABLE t_modify_order_by;
