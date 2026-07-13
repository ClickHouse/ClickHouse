SET enable_variant_type = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Variant(Int,Int)) ENGINE = Memory();
INSERT INTO TABLE FUNCTION file('data_03314.file', 'RowBinary', 'c0 Variant(Int,Int)') SELECT c0 FROM t0;
INSERT INTO TABLE t0 (c0) SELECT * FROM file('data_03314.file', 'RowBinary', 'c0 Variant(Int,Int)');
