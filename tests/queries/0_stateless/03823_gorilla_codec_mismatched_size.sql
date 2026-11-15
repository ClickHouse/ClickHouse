-- Gorilla codec with data_bytes_size larger than the column type.
-- When all data fits in the "skip" section (uncompressed_size < data_bytes_size),
-- decompression should still work correctly.

SET allow_suspicious_codecs = 1;

DROP TABLE IF EXISTS t_gorilla_mismatched;

CREATE TABLE t_gorilla_mismatched (c0 Int16 CODEC(Gorilla(4))) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_gorilla_mismatched (c0) VALUES (1);
SELECT * FROM t_gorilla_mismatched;

-- Also test with codec pipeline
DROP TABLE IF EXISTS t_gorilla_mismatched;
CREATE TABLE t_gorilla_mismatched (c0 Int16 CODEC(Gorilla(4), ZSTD)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_gorilla_mismatched (c0) VALUES (42);
SELECT * FROM t_gorilla_mismatched;

-- Test with Gorilla(8) on Int16
DROP TABLE IF EXISTS t_gorilla_mismatched;
CREATE TABLE t_gorilla_mismatched (c0 Int16 CODEC(Gorilla(8))) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_gorilla_mismatched (c0) VALUES (100);
SELECT * FROM t_gorilla_mismatched;

-- Test with compact parts (the original issue scenario)
DROP TABLE IF EXISTS t_gorilla_mismatched;
CREATE TABLE t_gorilla_mismatched (c0 Int16 CODEC(Gorilla(4))) ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 10000000000;
INSERT INTO t_gorilla_mismatched (c0) VALUES (7);
SELECT * FROM t_gorilla_mismatched;

DROP TABLE IF EXISTS t_gorilla_mismatched;
