SET allow_experimental_dynamic_type = 1;
SET allow_dynamic_type_in_join_keys = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Tuple(c1 Int,c2 Dynamic)) ENGINE = Memory();
SELECT 1 FROM t0 tx JOIN t0 ty ON tx.c0 = ty.c0;
DROP TABLE t0;

