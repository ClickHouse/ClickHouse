-- The trivial `GROUP BY ... LIMIT` optimization fires for projections with aggregate
-- functions: once any aggregation stream exceeds `LIMIT + OFFSET` distinct keys, all
-- streams are restricted to a single shared set of kept keys, so the aggregate values
-- of the returned keys must be exact. A per-stream cutoff (the pre-existing behavior of
-- `max_rows_to_group_by` with `group_by_overflow_mode = 'any'`) would undercount them:
-- a key kept by one stream and rejected by another loses the other stream's rows.
--
-- Every query compares the limited aggregation against the full aggregation of the same
-- data (join on the group key): the kept keys are an unspecified subset, but their values
-- must match the full aggregation exactly.
--
-- 997 keys (prime) interleaved over the input so that the parallel streams (which read
-- contiguous ranges of `numbers_mt`) meet different keys first and would keep different
-- per-stream key sets. `max_block_size` is small so the cutoff trips early, after each
-- stream has aggregated only a fraction of its rows. The keys are cast to `UInt64`
-- because for the fixed hash map methods (8/16-bit keys) the cutoff intentionally stays
-- inert (the tables are bounded by the key space; see the last query).

SET optimize_trivial_group_by_limit_query = 1;
SET max_threads = 16;
SET max_block_size = 1000;

-- A user-supplied equal cap with the default `throw` mode must retain its exception
-- contract; the optimization must not replace it with the approximate `any` mode.
SELECT toString(number), count() FROM numbers(10) GROUP BY number LIMIT 5 SETTINGS max_rows_to_group_by = 5; -- { serverError TOO_MANY_ROWS }

-- UInt64 key, count + sum.
WITH lim AS (SELECT toUInt64(number % 997) AS k, count() AS c, sum(number) AS s FROM numbers_mt(100000) GROUP BY k LIMIT 10),
     tru AS (SELECT toUInt64(number % 997) AS k, count() AS c, sum(number) AS s FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c OR lim.s != tru.s) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- Single count(): the aggregator uses the specialized inline count representation.
WITH lim AS (SELECT toUInt64(number % 997) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 10),
     tru AS (SELECT toUInt64(number % 997) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- String key, min/max over strings (aggregate states with arena-allocated data).
WITH lim AS (SELECT toString(number % 997) AS k, min(toString(number)) AS mn, max(toString(number)) AS mx FROM numbers_mt(100000) GROUP BY k LIMIT 10),
     tru AS (SELECT toString(number % 997) AS k, min(toString(number)) AS mn, max(toString(number)) AS mx FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.mn != tru.mn OR lim.mx != tru.mx) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- Two keys (the ClickBench Q17 shape), uniqExact (a state with its own allocations
-- and a non-trivial destructor, exercising the destruction of the dropped states).
WITH lim AS (SELECT toUInt64(number % 89) AS k1, toUInt64(number % 101) AS k2, count() AS c, uniqExact(number % 7) AS u FROM numbers_mt(100000) GROUP BY k1, k2 LIMIT 10),
     tru AS (SELECT toUInt64(number % 89) AS k1, toUInt64(number % 101) AS k2, count() AS c, uniqExact(number % 7) AS u FROM numbers_mt(100000) GROUP BY k1, k2)
SELECT count(), countIf(lim.c != tru.c OR lim.u != tru.u) FROM lim INNER JOIN tru ON lim.k1 = tru.k1 AND lim.k2 = tru.k2;

-- LowCardinality(String) key.
WITH lim AS (SELECT toLowCardinality(toString(number % 997)) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 10),
     tru AS (SELECT toLowCardinality(toString(number % 997)) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- Nullable key (the null key is one of the groups).
WITH lim AS (SELECT nullIf(toUInt64(number % 997), 3) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 10),
     tru AS (SELECT nullIf(toUInt64(number % 997), 3) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON ifNull(lim.k, 997) = ifNull(tru.k, 997);

-- OFFSET contributes to the cap: `LIMIT 7 OFFSET 3` keeps 10 keys and returns 7 of them.
WITH lim AS (SELECT toUInt64(number % 997) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 7 OFFSET 3),
     tru AS (SELECT toUInt64(number % 997) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- Fewer distinct keys than the limit: the cutoff never freezes, the result is complete.
SELECT count(), countIf(c != 20000) FROM (SELECT toUInt64(number % 5) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 100);

-- Single stream: trivially exact (the per-stream and the shared cutoff coincide).
WITH lim AS (SELECT toUInt64(number % 997) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 10 SETTINGS max_threads = 1),
     tru AS (SELECT toUInt64(number % 997) AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- Distributed query: the aggregation is split between the shards and the initiator, so the
-- cutoff must stay off (per-shard kept keys would undercount across shards) and the values
-- must still be exact.
WITH lim AS (SELECT k, count() AS c FROM remote('127.0.0.{1,2}', view(SELECT toUInt64(number % 97) AS k FROM numbers(10000))) GROUP BY k LIMIT 10),
     tru AS (SELECT k, count() AS c FROM remote('127.0.0.{1,2}', view(SELECT toUInt64(number % 97) AS k FROM numbers(10000))) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON lim.k = tru.k;

-- 8/16-bit keys use fixed hash maps bounded by the key space, where the cutoff stays
-- inert: the aggregation is complete and exact (all 997 groups exist; the implicit-zero
-- fixed maps also cannot represent a kept key with a zero inline count() state).
WITH lim AS (SELECT number % 997 AS k, count() AS c FROM numbers_mt(100000) GROUP BY k LIMIT 10),
     tru AS (SELECT number % 997 AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)
SELECT count(), countIf(lim.c != tru.c) FROM lim INNER JOIN tru ON lim.k = tru.k;
