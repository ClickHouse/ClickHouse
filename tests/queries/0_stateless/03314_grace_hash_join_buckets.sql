DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Memory();
SELECT 1 FROM t0 JOIN t0 tx ON tx.c0 = t0.c0 SETTINGS join_algorithm = 'grace_hash', grace_hash_join_max_buckets = 0; -- { clientError BAD_ARGUMENTS }
