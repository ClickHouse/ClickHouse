SET allow_suspicious_primary_key = 1;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Tuple(a Int32, b Nullable(Int32)), c1 Int32) ENGINE = SummingMergeTree() ORDER BY c1;
INSERT INTO t0 VALUES ((1,2), 0);
INSERT INTO t0 VALUES ((3,4), 0);
SELECT c0 FROM t0 FINAL;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Tuple(a Int32, b Nullable(Int32)), c1 Int32) ENGINE = CoalescingMergeTree() ORDER BY c1;
INSERT INTO t0 VALUES ((1,2), 0); -- return this one because tuple has not a nullable type so we can not aggregate by tuple columns
INSERT INTO t0 VALUES ((3,4), 0);
SELECT c0 FROM t0 FINAL;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Array(Nullable(Int32)), c1 Int32) ENGINE = SummingMergeTree() ORDER BY c1;
INSERT INTO t0 VALUES ([1,2], 0);
INSERT INTO t0 VALUES ([3,4], 0);
SELECT c0 FROM t0 FINAL;


DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Array(Nullable(Int32)), c1 Int32) ENGINE = CoalescingMergeTree() ORDER BY c1;
INSERT INTO t0 VALUES ([1,2], 0); -- return this one because array has not a nullable type so we can not aggregate by tuple columns
INSERT INTO t0 VALUES ([3,4], 0);
SELECT c0 FROM t0 FINAL;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Tuple) ENGINE = CoalescingMergeTree() ORDER BY tuple();
INSERT INTO t0 (c0) VALUES (());
SELECT c0 FROM t0 FINAL;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Tuple) ENGINE = SummingMergeTree() ORDER BY tuple();
INSERT INTO t0 (c0) VALUES (());
SELECT c0 FROM t0 FINAL;
