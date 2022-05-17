DROP TABLE IF EXISTS t_ttl_non_deterministic;

CREATE TABLE t_ttl_non_deterministic(A Int64)
ENGINE = MergeTree ORDER BY A TTL now() + toIntervalMonth(1); -- {serverError BAD_ARGUMENTS}

CREATE TABLE t_ttl_non_deterministic(A Int64) ENGINE = MergeTree ORDER BY A;
ALTER TABLE t_ttl_non_deterministic MODIFY TTL now() + toIntervalMonth(1); -- {serverError BAD_ARGUMENTS}
DROP TABLE t_ttl_non_deterministic;

CREATE TABLE t_ttl_non_deterministic(A Int64, B Int64 TTL now() + toIntervalMonth(1))
ENGINE = MergeTree ORDER BY A; -- {serverError BAD_ARGUMENTS}

CREATE TABLE t_ttl_non_deterministic(A Int64, B Int64) ENGINE = MergeTree ORDER BY A;
ALTER TABLE t_ttl_non_deterministic MODIFY COLUMN B Int64 TTL now() + toIntervalMonth(1); -- {serverError BAD_ARGUMENTS}
DROP TABLE t_ttl_non_deterministic;
