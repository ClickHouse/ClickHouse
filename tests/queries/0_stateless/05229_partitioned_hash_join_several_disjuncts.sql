-- Joins with several disjuncts (`ON a OR b`) on the partitioned hash join: one table per clause over one block store,
-- a right row emitted once however many clauses reach it, used flags kept per right-table row.

SET max_threads = 8;
SET parallel_hash_join_threshold = 1;
SET max_block_size = 8192;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_or_l;
DROP TABLE IF EXISTS t_or_r;

CREATE TABLE t_or_l (a UInt64, b UInt64, c UInt64, n Nullable(UInt64), s String, v UInt8) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_or_r (a UInt64, b UInt64, c UInt64, n Nullable(UInt64), s String, v UInt8) ENGINE = MergeTree ORDER BY a;

-- The left side: 50k rows; the right side: 200k rows, so the build partitions and fills on several streams.
INSERT INTO t_or_l SELECT number, number * 3, number % 1000, if(number % 7 = 0, NULL, number), toString(number % 5000), number % 100 FROM numbers(50000);
INSERT INTO t_or_r SELECT number + 25000, number * 2, (number * 13) % 1000, if(number % 11 = 0, NULL, number + 50), toString(number % 7000), number % 100 FROM numbers(200000);

-- The build must fill in parallel, as it does for `parallel_hash` (04893).
SELECT 'parallel fill', coalesce(
    nullIf(max(toUInt64OrZero(extract(explain, 'FillingRightJoinSide × (\\d+)'))), 0),
    countIf(explain LIKE '%FillingRightJoinSide%')) > 1
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM t_or_l AS l FULL JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b
    SETTINGS join_algorithm = 'hash'
);

-- The non-joined rows of a FULL join with per-row flags are emitted on several streams too.
SELECT 'parallel non-joined', countIf(explain LIKE '%NonJoinedBlocksTransform%') > 1
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM t_or_l AS l FULL JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b
    SETTINGS join_algorithm = 'hash'
);

-- Two and three disjuncts, every kind and strictness the grammar allows.
SELECT 'inner all or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l INNER JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'inner all or3', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l INNER JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b OR l.c = r.c SETTINGS join_algorithm = 'hash') AS pa);
-- ANY claims a right row for the first left row that reaches it, so with several probe threads the pairs depend on the
-- thread order (on `hash` too). One thread makes them deterministic, and the two algorithms then agree exactly.
SELECT 'inner any or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l ANY INNER JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash', max_threads = 1) AS pa);
SELECT 'left all or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l LEFT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'left any or3', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l ANY LEFT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b OR l.c = r.c SETTINGS join_algorithm = 'hash', max_threads = 1) AS pa);
SELECT 'left semi or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, l.c))) FROM t_or_l AS l SEMI LEFT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'left anti or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, l.c))) FROM t_or_l AS l ANTI LEFT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'right all or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l RIGHT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'right all or3', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l RIGHT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b OR l.c = r.c SETTINGS join_algorithm = 'hash') AS pa);
-- RIGHT ANY and SEMI claim each right row once, through the per-row flags (04869).
SELECT 'right any or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(r.a, r.b, r.c)), uniqExact(r.a, r.b)) FROM t_or_l AS l ANY RIGHT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'right semi or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(r.a, r.b, r.c)), uniqExact(r.a, r.b)) FROM t_or_l AS l SEMI RIGHT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'right anti or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(r.a, r.b, r.c))) FROM t_or_l AS l ANTI RIGHT JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'full all or2', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l FULL JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'full all or3', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l FULL JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b OR l.c = r.c SETTINGS join_algorithm = 'hash') AS pa);

-- Null keys (a NULL never matches), `join_use_nulls`, and a per-clause ON filter.
SELECT 'full all nullable', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, ifNull(l.n, 0), r.a, ifNull(r.n, 0)))) FROM t_or_l AS l FULL JOIN t_or_r AS r ON l.n = r.n OR l.b = r.b SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'left all use_nulls', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, ifNull(r.a, 0), ifNull(r.c, 0)))) FROM t_or_l AS l LEFT JOIN t_or_r AS r ON l.a = r.a OR l.s = r.s SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS pa);
SELECT 'right all filter', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l RIGHT JOIN t_or_r AS r ON (l.a = r.a AND l.v < 50) OR (l.b = r.b AND r.v > 20) SETTINGS join_algorithm = 'hash') AS pa);
-- Mixed non-equi condition next to the disjuncts, on the kinds whose flags are per row anyway.
SELECT 'full all mixed', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l FULL JOIN t_or_r AS r ON (l.a = r.a OR l.b = r.b) AND l.v < r.v SETTINGS join_algorithm = 'hash') AS pa);
-- The right keys of every clause come out as columns (05023).
SELECT 'right keys', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(r.a, r.b, r.c, r.s))) FROM t_or_l AS l INNER JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b OR l.s = r.s SETTINGS join_algorithm = 'hash') AS pa);
-- One thread: the single fill thread builds one table per clause as the blocks arrive.
SELECT 'full all or2 one thread', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, r.a, r.b, r.c))) FROM t_or_l AS l FULL JOIN t_or_r AS r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash', max_threads = 1) AS pa);


-- A dictionary on the right side of a RIGHT / FULL join with a mixed ON condition: the probe dispatch needs the RIGHT/FULL
-- arms under `preferUseMapsAll`.
DROP DICTIONARY IF EXISTS d_or_r;
CREATE DICTIONARY d_or_r (a UInt64, b UInt64, c UInt64, v UInt8) PRIMARY KEY a
SOURCE(CLICKHOUSE(TABLE 't_or_r' DATABASE currentDatabase())) LAYOUT(HASHED()) LIFETIME(0);
SELECT 'right mixed dictionary', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, d.a, d.b, d.c))) FROM t_or_l AS l RIGHT JOIN d_or_r AS d ON l.a = d.a AND l.v < d.v SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'full mixed dictionary', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, d.a, d.b, d.c))) FROM t_or_l AS l FULL JOIN d_or_r AS d ON l.a = d.a AND l.v < d.v SETTINGS join_algorithm = 'hash') AS pa);
SELECT 'right or dictionary', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(l.a, l.b, d.a, d.b, d.c))) FROM t_or_l AS l RIGHT JOIN d_or_r AS d ON l.a = d.a OR l.b = d.b SETTINGS join_algorithm = 'hash') AS pa);
DROP DICTIONARY d_or_r;

DROP TABLE t_or_l;
DROP TABLE t_or_r;
