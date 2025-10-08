-- { echo ON }

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 String) ENGINE = MergeTree() ORDER BY (c0) SETTINGS write_marks_for_substreams_in_compact_parts = 0;

INSERT INTO TABLE t0 (c0) VALUES('');

SELECT c0.size FROM t0;

DROP TABLE t0;
