DROP TABLE IF EXISTS nums;

CREATE TABLE nums(n UInt32) ENGINE = Memory;

INSERT INTO nums VALUES (4),(2),(1),(3);

SELECT quantilesExactExclusive(0.1, 0.9)(n) FROM nums;
SELECT quantilesExactInclusive(0, 1)(n) FROM nums;

DROP TABLE nums;
