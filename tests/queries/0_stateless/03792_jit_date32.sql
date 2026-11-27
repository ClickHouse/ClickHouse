DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Date32, c1 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t0 UPDATE c0 = '2026-01-01' WHERE c1 = 1 SETTINGS min_count_to_compile_expression = 0;
DROP TABLE t0;
