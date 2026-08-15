-- The part stores `c` as Array(String), while table metadata says Array(LowCardinality(String)).
-- Filling `arr`, which is absent from the part, walks `c` to collect its offsets. The check must
-- diagnose the mismatch before `SerializationLowCardinality` casts the nested column.

DROP TABLE IF EXISTS t_low_cardinality_missing;

CREATE TABLE t_low_cardinality_missing (k UInt64, c Array(String)) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_low_cardinality_missing VALUES (1, ['a']);

ALTER TABLE t_low_cardinality_missing MODIFY COLUMN c Array(LowCardinality(String));
ALTER TABLE t_low_cardinality_missing ADD COLUMN arr Array(UInt64);

SELECT c, arr FROM t_low_cardinality_missing; -- { serverError LOGICAL_ERROR }

DROP TABLE t_low_cardinality_missing;
