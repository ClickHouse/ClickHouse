-- Tests for storing `Tuple` and `Nullable(Tuple)` columns of the hash table payload in row-major form.
-- A tuple of fixed-width elements is laid out as its elements.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_spill;

CREATE TABLE left (k Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right
(
    k Int64,
    p Tuple(x Float64, y Float64),
    t Tuple(a UInt8, b Tuple(c Int64, d Date)),
    n Tuple(Nullable(Int32), UInt8),
    nt Nullable(Tuple(UInt16, Int64))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left SELECT number FROM numbers(10);
INSERT INTO right SELECT
    number + 7,
    (number, number + 0.5),
    (number, (-number, toDate('2024-01-01') + number)),
    (if(number = 2, NULL, number), number % 2),
    if(number = 1, NULL, (number, number * 10))
FROM numbers(5);
INSERT INTO right VALUES (7, (9, 9.5), (9, (-9, '2024-02-01')), (NULL, 1), NULL);

CREATE TABLE right_spill AS right;
INSERT INTO right_spill SELECT
    number + 7,
    (number, number + 0.5),
    (number, (-number, toDate('2024-01-01') + number)),
    (if(number = 2, NULL, number), number % 2),
    if(number = 1, NULL, (number, number * 10))
FROM numbers(20000);

SET join_algorithm = 'hash';
SET join_use_nulls = 0;

-- Pin planner settings
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 0;
SET enable_hash_join_row_store = 1;
SET min_rows_ratio_for_hash_join_row_store = 0;

SELECT '--- INNER ---';
SELECT l.k, r.k, r.p, r.t, r.n, r.nt FROM left l INNER JOIN right r ON l.k = r.k ORDER BY l.k, r.p
SETTINGS log_comment = 'rs_tuples';

SELECT '--- LEFT ---';
SELECT l.k, r.k, r.p, r.t, r.n, r.nt FROM left l LEFT JOIN right r ON l.k = r.k ORDER BY l.k, r.p;

SELECT '--- RIGHT ---';
SELECT l.k, r.k, r.p, r.t, r.n, r.nt FROM left l RIGHT JOIN right r ON l.k = r.k ORDER BY l.k, r.p;

SELECT '--- LEFT parallel_hash ---';
SELECT l.k, r.k, r.p, r.t, r.n, r.nt FROM left l LEFT JOIN right r ON l.k = r.k ORDER BY l.k, r.p
SETTINGS join_algorithm = 'parallel_hash';

SELECT '--- INNER spilled ---';
SELECT count(), sum(cityHash64(toString((r.p, r.t, r.n, r.nt)))), countIf(tupleElement(r.n, 1) IS NULL), countIf(r.nt IS NULL)
FROM (SELECT number % 20000 + 7 AS k FROM numbers(40000)) l INNER JOIN right_spill r ON l.k = r.k
SETTINGS max_bytes_before_external_join = 100000, max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 1, log_comment = 'rs_tuples_spill';

SYSTEM FLUSH LOGS text_log, query_log;

SELECT 'rs_tuples_spill', ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND log_comment = 'rs_tuples_spill' AND type = 'QueryFinish';

SELECT 'rs_tuples', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_tuples' AND type = 'QueryFinish');

DROP TABLE right_spill;
DROP TABLE right;
DROP TABLE left;
