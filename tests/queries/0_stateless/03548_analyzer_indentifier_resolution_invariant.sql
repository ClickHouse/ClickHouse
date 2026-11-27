SET allow_experimental_analyzer = 1;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;
CREATE VIEW v0 AS (SELECT 1 AS a0, (1) IN a0 FROM t0 tx JOIN t0 ty ON 1 CROSS JOIN t0 tz); -- { serverError UNKNOWN_IDENTIFIER }
