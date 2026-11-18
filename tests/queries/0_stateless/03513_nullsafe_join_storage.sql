DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = Join(ALL, INNER, c0);
INSERT INTO t0 VALUES (1), (2), (3), (4), (5);

SELECT 1 FROM t0 JOIN t0 tx ON t0.c0 <=> tx.c0; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

EXPLAIN AST optimize=1 SELECT 1 FROM t0 JOIN t0 tx ON t0.c0 <=> tx.c0 PASTE JOIN t0 ty; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
