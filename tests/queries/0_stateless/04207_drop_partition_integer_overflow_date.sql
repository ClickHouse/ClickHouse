-- Regression test: ALTER TABLE ... DROP PARTITION with an integer literal that
-- overflows the Date underlying type (UInt16) used to silently truncate the
-- value modulo 65536, producing a partition ID that could collide with an
-- unrelated existing part and surface as LOGICAL_ERROR
-- ("Parsed partition value: ... doesn't match partition value for an existing
-- part with the same partition ID: ...").

DROP TABLE IF EXISTS t_drop_partition_overflow;

CREATE TABLE t_drop_partition_overflow (d Date, x UInt32)
ENGINE = MergeTree PARTITION BY d ORDER BY x;

-- 20200523 mod 65536 = 15435 = '2012-04-05', which used to collide with this part.
INSERT INTO t_drop_partition_overflow VALUES ('2012-04-05', 1);

-- Must throw a clean ARGUMENT_OUT_OF_BOUND, not LOGICAL_ERROR.
-- UInt64 path (bare positive literal).
ALTER TABLE t_drop_partition_overflow DROP PARTITION 20200523; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Int64 path: ParserPartition rejects bare non-`tuple` functions, but `tuple(...)`
-- is unwrapped by `getPartitionIDFromQuery` and the inner expression is evaluated,
-- so `toInt64(...)` reaches `convertFieldToType` as `Field::Int64`.
ALTER TABLE t_drop_partition_overflow DROP PARTITION tuple(toInt64(20200523)); -- { serverError ARGUMENT_OUT_OF_BOUND }
ALTER TABLE t_drop_partition_overflow DROP PARTITION tuple(CAST(20200523, 'Int64')); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The original part must still be present.
SELECT count() FROM t_drop_partition_overflow;

-- Sanity check: dropping by the correctly-typed partition value still works.
ALTER TABLE t_drop_partition_overflow DROP PARTITION '2012-04-05';
SELECT count() FROM t_drop_partition_overflow;

DROP TABLE t_drop_partition_overflow;

-- Complex partition key: expression evaluation reaches `convertFieldToType`
-- directly (no tuple unwrap), so any Int64-producing expression on the Date
-- column triggers the same bug.
DROP TABLE IF EXISTS t_drop_partition_overflow_complex;

CREATE TABLE t_drop_partition_overflow_complex (d Date, x UInt32)
ENGINE = MergeTree PARTITION BY (d, x) ORDER BY x;

INSERT INTO t_drop_partition_overflow_complex VALUES ('2012-04-05', 1);

ALTER TABLE t_drop_partition_overflow_complex DROP PARTITION (toInt64(20200523), 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_drop_partition_overflow_complex;

DROP TABLE t_drop_partition_overflow_complex;
