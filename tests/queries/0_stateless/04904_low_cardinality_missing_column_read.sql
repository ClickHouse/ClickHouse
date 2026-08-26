-- Keep a part written before `LowCardinality` was introduced, then read it together with an
-- Array column missing from that part. The missing column makes `fillMissingColumns` collect
-- offsets from `c`; the reader converts the old nested String data to a low-cardinality
-- dictionary, exercising the `DataTypeLowCardinality` branch of
-- `columnMatchesTypeStructure`.

DROP TABLE IF EXISTS t_low_cardinality_missing;

CREATE TABLE t_low_cardinality_missing (k UInt64, c Array(String)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_low_cardinality_missing VALUES (1, ['a']);

SYSTEM STOP MERGES t_low_cardinality_missing;
ALTER TABLE t_low_cardinality_missing MODIFY COLUMN c Array(LowCardinality(String)) SETTINGS alter_sync = 0;
ALTER TABLE t_low_cardinality_missing ADD COLUMN arr Array(UInt64);

SELECT c, arr FROM t_low_cardinality_missing;

DROP TABLE t_low_cardinality_missing;
