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

-- `UInt64` partition-value path (positive literal): originally threw LOGICAL_ERROR.
OPTIMIZE TABLE t_stid_3993_36c1 PARTITION 0 FINAL;

SELECT count(), uniqExact(_part) FROM t_stid_3993_36c1;

-- `Int64` partition-value path (negative literal): the same `convertFieldToTypeImpl`
-- branch used to return the bare `Int64` `Field` unchanged, so out-of-range values
-- were silently truncated by the downstream `Time` serializer. It is now routed
-- through `convertNumericType<Int32>` for symmetric range-checking. An in-range
-- value must still succeed (no exception).
OPTIMIZE TABLE t_stid_3993_36c1 PARTITION -1 FINAL;

-- Out-of-range `Int64` partition value must now be rejected with a clear
-- `ARGUMENT_OUT_OF_BOUND` error from `convertFieldToTypeOrThrow` instead of
-- being silently truncated downstream. `-2147483649` is one below `INT32_MIN`.
OPTIMIZE TABLE t_stid_3993_36c1 PARTITION -2147483649 FINAL; -- { serverError ARGUMENT_OUT_OF_BOUND }

DROP TABLE t_stid_3993_36c1;
