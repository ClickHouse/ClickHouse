-- Regression test: an ASOF JOIN whose ON section yields no equality key (here a single
-- inequality, optionally wrapped in a redundant one-argument `and`) must report a proper
-- exception instead of failing the `analyzed_join.oneDisjunct()` assertion in a debug build.

SET enable_analyzer = 0;

DROP TABLE IF EXISTS t0_04303;
DROP TABLE IF EXISTS t1_04303;

CREATE TABLE t0_04303 (x Int32, y Int32) ENGINE = Memory;
CREATE TABLE t1_04303 (x Int32, y Int32) ENGINE = Memory;

SELECT * FROM t0_04303 ASOF LEFT JOIN t1_04303 ON and((t0_04303.y > t1_04303.y)); -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT * FROM t0_04303 ASOF LEFT JOIN t1_04303 ON (t0_04303.y > t1_04303.y); -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE t0_04303;
DROP TABLE t1_04303;
