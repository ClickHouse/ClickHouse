-- Partition operations between tables that differ in the `Quantized(...)` codec of a column are rejected. Otherwise the
-- parts of the source table would be read with the serialization of the destination table, e.g. vectors attached into
-- a coded column from a table without the codec would be silently read (and merged) as zero vectors.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_qc_plain;
DROP TABLE IF EXISTS t_qc_coded;
DROP TABLE IF EXISTS t_qc_coded_other;
DROP TABLE IF EXISTS t_qc_coded_same;

CREATE TABLE t_qc_plain (id UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_qc_coded (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 8))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_qc_coded_other (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 16))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_qc_coded_same (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 8))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_qc_plain VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [8, 7, 6, 5, 4, 3, 2, 1]), (3, [3, 3, 3, 3, 3, 3, 3, 3]);
INSERT INTO t_qc_coded_same VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [8, 7, 6, 5, 4, 3, 2, 1]), (3, [3, 3, 3, 3, 3, 3, 3, 3]);

-- plain -> coded
ALTER TABLE t_qc_coded ATTACH PARTITION tuple() FROM t_qc_plain; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE t_qc_coded REPLACE PARTITION tuple() FROM t_qc_plain; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE t_qc_plain MOVE PARTITION tuple() TO TABLE t_qc_coded; -- { serverError INCOMPATIBLE_COLUMNS }

-- coded -> plain
ALTER TABLE t_qc_plain ATTACH PARTITION tuple() FROM t_qc_coded_same; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE t_qc_coded_same MOVE PARTITION tuple() TO TABLE t_qc_plain; -- { serverError INCOMPATIBLE_COLUMNS }

-- coded -> coded with a different codec
ALTER TABLE t_qc_coded_other ATTACH PARTITION tuple() FROM t_qc_coded_same; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT 'destination_empty', count() FROM t_qc_coded;
SELECT 'source_kept', id, vec FROM t_qc_plain ORDER BY id;

-- coded -> coded with the same codec is allowed and keeps the values, also after a merge.
ALTER TABLE t_qc_coded ATTACH PARTITION tuple() FROM t_qc_coded_same;
SELECT 'attached', id, vec FROM t_qc_coded ORDER BY id;
OPTIMIZE TABLE t_qc_coded FINAL;
SELECT 'merged', id, vec FROM t_qc_coded ORDER BY id;

-- The data can be moved into a coded table with INSERT ... SELECT.
TRUNCATE TABLE t_qc_coded;
INSERT INTO t_qc_coded SELECT * FROM t_qc_plain;
SELECT 'inserted', id, vec FROM t_qc_coded ORDER BY id;

DROP TABLE t_qc_plain;
DROP TABLE t_qc_coded;
DROP TABLE t_qc_coded_other;
DROP TABLE t_qc_coded_same;
