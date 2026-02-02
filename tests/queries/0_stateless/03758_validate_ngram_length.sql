DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 String) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES ('a');
ALTER TABLE t0 ADD INDEX i0 c0 TYPE ngrambf_v1(0, 1, 1, 1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t0 ADD INDEX i0 c0 TYPE ngrambf_v1(-1, 1, 1, 1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t0 ADD INDEX i0 c0 TYPE ngrambf_v1(18_446_744_073_709_551_616, 1, 1, 1); -- { serverError BAD_GET }
ALTER TABLE t0 ADD INDEX i1 c0 TYPE ngrambf_v1(8, 1, 1, 1);
ALTER TABLE t0 ADD INDEX i2 c0 TYPE ngrambf_v1(9, 1, 1, 1);

SELECT c0 FROM t0;
