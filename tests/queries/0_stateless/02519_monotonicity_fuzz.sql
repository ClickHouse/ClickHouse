DROP TABLE IF EXISTS t;
CREATE TABLE t (x Decimal(18, 3)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1.1);
SELECT * FROM t WHERE toUInt64(x) = 1;
DROP TABLE t;

CREATE TABLE t (x DateTime64(3)) ENGINE = MergeTree ORDER BY x;
-- An unquoted number is a Unix timestamp in seconds, so `1` is one second since the epoch.
INSERT INTO t VALUES (1);
SELECT x::UInt64 FROM t WHERE toUInt64(x) = 1;
DROP TABLE t;
