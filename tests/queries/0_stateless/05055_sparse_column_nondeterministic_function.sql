-- The sparse fast path evaluates a function once for the column's default value and repeats that one
-- result across every default row. For a nondeterministic function that hands the whole default run a
-- single shared value, so `generateUUIDv4` returned one "unique" id for hundreds of rows.
-- https://github.com/ClickHouse/ClickHouse/issues/117209
--
-- The constant-argument form of the same mistake is fixed separately in
-- https://github.com/ClickHouse/ClickHouse/pull/117358

DROP TABLE IF EXISTS t_sparse_nondeterministic;

CREATE TABLE t_sparse_nondeterministic (id UInt64, s UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

-- 99% of `s` is the default, which is what makes it serialize sparsely.
INSERT INTO t_sparse_nondeterministic SELECT number, if(number % 100 = 0, number, 0) FROM numbers(1000);

SELECT 'the column really is sparse';
-- Without this the rest of the test proves nothing.
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_nondeterministic' AND column = 's' AND active;

SELECT 'generators that never read their argument';
-- These opt out of the sparse handling entirely, so the column is not materialized on their behalf.
-- `generateULID` is covered by the same opt-out but is left out here: ULID is not available in the
-- fast test build.
SELECT uniqExact(generateUUIDv4(s)) FROM t_sparse_nondeterministic;
SELECT uniqExact(rand(s)) > 900 FROM t_sparse_nondeterministic;
SELECT count() BETWEEN 400 AND 600 FROM t_sparse_nondeterministic WHERE rand(s) % 2 = 0;

SELECT 'a generator that does read its argument';
-- This one keeps the default sparse handling and relies on the guard in
-- `executeWithoutReplicatedColumns` instead, so it covers the other half of the fix.
SELECT uniqExact(generateSnowflakeID(s)) FROM t_sparse_nondeterministic;

SELECT 'deterministic functions keep the fast path';
SELECT uniqExact(s + 1), sum(s + 1) FROM t_sparse_nondeterministic;

DROP TABLE t_sparse_nondeterministic;
