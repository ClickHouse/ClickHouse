SET limit = 4;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Tuple(), c1 Array(Int)) ENGINE = AggregatingMergeTree() ORDER BY tuple() SETTINGS allow_suspicious_primary_key=1;
INSERT INTO TABLE t0 (c0, c1) VALUES ((), [1]), ((), []), ((), [2]);
SELECT c0 FROM t0 FINAL ORDER BY c1;
DROP TABLE t0;

