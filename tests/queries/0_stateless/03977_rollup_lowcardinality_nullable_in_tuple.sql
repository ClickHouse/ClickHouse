-- Regression test: ROLLUP with LowCardinality(Nullable) inside Nullable(Tuple) caused
-- a logical error due to incorrect getSerializedValueSize in ColumnLowCardinality/ColumnUnique.
-- The serialized size didn't account for the null flag byte, causing key deserialization corruption.

SET allow_experimental_nullable_tuple_type = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_rollup_lc_nullable;

CREATE TABLE t_rollup_lc_nullable (value Nullable(Tuple(LowCardinality(Nullable(Int64))))) ENGINE = Memory;
INSERT INTO t_rollup_lc_nullable VALUES ((NULL));

SELECT 1 FROM t_rollup_lc_nullable GROUP BY value, 'foo' WITH ROLLUP;

DROP TABLE t_rollup_lc_nullable;
