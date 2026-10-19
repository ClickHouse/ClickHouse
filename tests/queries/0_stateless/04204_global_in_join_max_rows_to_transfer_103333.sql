-- Tags: distributed
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103333
--
-- The `max_rows_to_transfer` and `max_bytes_to_transfer` settings limit how many
-- rows / bytes the initiator is allowed to materialize when sending an external
-- table for `GLOBAL IN` / `GLOBAL JOIN`. Under the old analyzer they were
-- enforced inside `CreatingSetsTransform`. The analyzer materialises the
-- subquery in `buildQueryTreeForShard` via `executeSubqueryNode` and used to
-- skip that check, silently ignoring the limit. This test guards against the
-- regression by asserting that the limit fires under both analyzers.

DROP TABLE IF EXISTS t1_local SYNC;
DROP TABLE IF EXISTS t2_local SYNC;
DROP TABLE IF EXISTS t1_dist SYNC;
DROP TABLE IF EXISTS t2_dist SYNC;

CREATE TABLE t1_local (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2_local (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t1_dist  (a Int32, b Int32) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t1_local, rand());
CREATE TABLE t2_dist  (a Int32, b Int32) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t2_local, rand());

-- Insert directly into the local tables instead of `INSERT INTO ... Distributed`
-- to keep the test deterministic: both shards in `test_cluster_two_shards` resolve
-- to this same physical server, so the data is visible from both shards through
-- the `Distributed` reader without depending on the asynchronous `INSERT` path.
INSERT INTO t1_local SELECT number, number + 1 FROM numbers(10000);
INSERT INTO t2_local SELECT number, number + 2 FROM numbers(100);

-- Analyzer (default) — must throw SET_SIZE_LIMIT_EXCEEDED (191).
SELECT t1.a, t2.b FROM t1_dist AS t1 GLOBAL JOIN t2_dist AS t2 ON t1.a = t2.a
SETTINGS max_rows_to_transfer = 10; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT count() FROM t1_dist WHERE a GLOBAL IN (SELECT a FROM t2_dist)
SETTINGS max_rows_to_transfer = 10; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Old analyzer — must throw the same error (sanity check that this code path
-- has always worked and we didn't break it).
SELECT t1.a, t2.b FROM t1_dist AS t1 GLOBAL JOIN t2_dist AS t2 ON t1.a = t2.a
SETTINGS max_rows_to_transfer = 10, enable_analyzer = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT count() FROM t1_dist WHERE a GLOBAL IN (SELECT a FROM t2_dist)
SETTINGS max_rows_to_transfer = 10, enable_analyzer = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `transfer_overflow_mode = break` must NOT throw; the result is allowed to be
-- truncated. We only check that the query completes under the analyzer.
SELECT count() >= 0 FROM t1_dist WHERE a GLOBAL IN (SELECT a FROM t2_dist)
SETTINGS max_rows_to_transfer = 10, transfer_overflow_mode = 'break';

-- Sanity: with the limit relaxed, the same queries succeed under the
-- analyzer. This verifies that we haven't broken the happy path.
SELECT count() FROM t1_dist AS t1 GLOBAL JOIN t2_dist AS t2 ON t1.a = t2.a
SETTINGS max_rows_to_transfer = 0;

SELECT count() FROM t1_dist WHERE a GLOBAL IN (SELECT a FROM t2_dist)
SETTINGS max_rows_to_transfer = 0;

-- `ColumnConst` regression: a subquery producing N rows of a single constant
-- value must be counted as the materialized N * sizeof(value) payload, not
-- as the single stored value. This guards against
-- https://github.com/ClickHouse/ClickHouse/pull/104119#discussion_r3189697129
-- where `chunk.bytes()` was reading the unmaterialized `ColumnConst` (one
-- stored entry only) and silently bypassed `max_bytes_to_transfer`.
--
-- The constants below sum to ~24 bytes per row when materialized (4-byte
-- `Int32` + 20-byte `String`), times 1000 rows = ~24000 bytes >> 100.
-- Without `materializeBlock` the limits transform would only see a few dozen
-- bytes of `ColumnConst` storage and let the query through silently.
-- We disable `enable_add_distinct_to_in_subqueries` here: the optimization
-- inserts a `DISTINCT` into the `GLOBAL IN` subquery and would collapse the
-- 1000 rows of `42` to a single row, which would defeat the regression we
-- want to assert (the materialized payload, not the deduplicated payload, is
-- what counts toward `max_bytes_to_transfer`).
SELECT count() FROM t1_dist AS t1
GLOBAL JOIN (
    SELECT toInt32(42) AS a, 'aaaaaaaaaaaaaaaaaaaa' AS s FROM numbers(1000)
) AS t2 ON t1.a = t2.a
SETTINGS max_rows_to_transfer = 0, max_bytes_to_transfer = 100,
    enable_add_distinct_to_in_subqueries = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT count() FROM t1_dist
WHERE a GLOBAL IN (
    SELECT toInt32(42) AS x FROM numbers(1000)
)
SETTINGS max_rows_to_transfer = 0, max_bytes_to_transfer = 100,
    enable_add_distinct_to_in_subqueries = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- And the same constant-column workload with the limit relaxed must still
-- succeed under the analyzer.
SELECT count() FROM t1_dist
WHERE a GLOBAL IN (
    SELECT toInt32(42) AS x FROM numbers(1000)
)
SETTINGS max_rows_to_transfer = 0, max_bytes_to_transfer = 0,
    enable_add_distinct_to_in_subqueries = 0;

DROP TABLE t1_local SYNC;
DROP TABLE t2_local SYNC;
DROP TABLE t1_dist SYNC;
DROP TABLE t2_dist SYNC;
