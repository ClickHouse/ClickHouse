-- A function that is not deterministic within the query must be evaluated per row, not once per
-- dictionary key of a `LowCardinality` argument: the column argument of `rand`, `generateUUIDv4` and
-- friends exists precisely to get an independent value per row.

DROP TABLE IF EXISTS t_lc_nondeterministic;
CREATE TABLE t_lc_nondeterministic (k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_nondeterministic SELECT toString(number % 10) FROM numbers(1000);

SELECT uniqExact(generateUUIDv4(k)), uniqExact(generateUUIDv7(k)), uniqExact(generateSnowflakeID(k)) FROM t_lc_nondeterministic;
SELECT uniqExact(rand64(k)), uniqExact(randCanonical(k)), uniqExact(randomString(16, k)) FROM t_lc_nondeterministic;

-- `rand` returns only 32 bits, so 1000 independent values still collide once in about ten thousand
-- runs: assert the result type instead, which is what the dictionary fast path keys off.
SELECT DISTINCT toTypeName(rand(k)) FROM t_lc_nondeterministic;

-- The values are persisted correctly too.
DROP TABLE IF EXISTS t_lc_nondeterministic_ids;
CREATE TABLE t_lc_nondeterministic_ids (u UUID) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_nondeterministic_ids SELECT generateUUIDv4(k) FROM t_lc_nondeterministic;
SELECT count(), uniqExact(u) FROM t_lc_nondeterministic_ids;

-- A deterministic function keeps evaluating on the dictionary and keeps its `LowCardinality` result.
SELECT DISTINCT toTypeName(upper(k)) FROM t_lc_nondeterministic;
SELECT uniqExact(upper(k)) FROM t_lc_nondeterministic;

DROP TABLE t_lc_nondeterministic;
DROP TABLE t_lc_nondeterministic_ids;
