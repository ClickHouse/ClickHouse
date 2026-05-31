-- Regression test for STID 3993-36c1.
--
-- `OPTIMIZE TABLE ... PARTITION 0 ...` against a table with a `Time`-typed
-- partition key used to throw LOGICAL_ERROR
--   "Parsed partition value: 00:00:00 doesn't match partition value for an
--    existing part with the same partition ID: 0_1_1_0"
-- because `convertFieldToTypeImpl` returned the bare `UInt64` `Field` for
-- `Time` partition values, while the on-disk partition value is read back as
-- an `Int64` `Field` (since `Time` is backed by `Int32`). The strict `Field`
-- type comparison in `MergeTreeData::getPartitionIDFromQuery` then fired.

DROP TABLE IF EXISTS t_stid_3993_36c1;

CREATE TABLE t_stid_3993_36c1 (id UInt64, p Time)
ENGINE = MergeTree PARTITION BY p ORDER BY id;

INSERT INTO t_stid_3993_36c1 SELECT number, 0 FROM numbers(10);

-- This previously threw LOGICAL_ERROR; should now succeed.
OPTIMIZE TABLE t_stid_3993_36c1 PARTITION 0 FINAL;

SELECT count(), uniqExact(_part) FROM t_stid_3993_36c1;

DROP TABLE t_stid_3993_36c1;
