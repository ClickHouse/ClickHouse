DROP TABLE IF EXISTS t0;
SET allow_suspicious_primary_key = 1;
CREATE TABLE t0 (c0 Dynamic) ENGINE = SummingMergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES ('a'::Enum('a' = 1)), (2);
SELECT c0 FROM t0 FINAL;
DROP TABLE t0;

