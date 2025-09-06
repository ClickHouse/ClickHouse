CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
ALTER TABLE t0 ADD CONSTRAINT c0 CHECK (SELECT 1);
SELECT 1 FROM t0 WHERE 1 = 1 SETTINGS optimize_substitute_columns = 1, convert_query_to_cnf = 1;
