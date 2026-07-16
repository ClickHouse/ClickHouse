-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/98579
-- Segfault in readBits when reading marks from a compact part during mutation

DROP TABLE IF EXISTS t0;

SET optimize_on_insert = 0;
CREATE TABLE t0 (c0 Decimal128(33) NOT NULL) ENGINE = AggregatingMergeTree() ORDER BY tuple();

INSERT INTO TABLE t0 (c0) SELECT toDecimal128('84.27312182962147570691056820064', 33) FROM numbers(1571);

SYSTEM PREWARM MARK CACHE t0;
SYSTEM UNLOAD PRIMARY KEY t0;
SYSTEM STOP MERGES t0;
SYSTEM PREWARM MARK CACHE t0;
SYSTEM LOAD PRIMARY KEY t0;

SET optimize_on_insert = 1;
INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Decimal128(33)', 12160759896411744424, 148, 80) LIMIT 2000;

SET max_threads = 12;
INSERT INTO TABLE t0 (c0) SELECT if(number % 10, CAST((-number) AS Decimal128(33)), CAST(number AS Decimal128(33))) FROM numbers(2337) GROUP BY number;
INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Decimal128(33)', 9028751427781862027, 219, 22) LIMIT 852;
INSERT INTO TABLE t0 (c0) SELECT toDecimal128('-4053.42025', 33) FROM numbers(363);
INSERT INTO TABLE t0 (c0) SELECT CAST(number % 21 AS Decimal128(33)) FROM numbers(934);

SYSTEM START MERGES t0;
ALTER TABLE t0 DELETE IN PARTITION tuple() WHERE equals(c0, 10951258226129012183::Int8) SETTINGS mutations_sync = 2;

SELECT count() > 0 FROM t0;

DROP TABLE t0;
