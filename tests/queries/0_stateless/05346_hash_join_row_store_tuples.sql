-- Tests for storing `Tuple` and `Nullable(Tuple)` columns of the hash table payload in row-major form.
-- A tuple of fixed-width elements is laid out as its elements.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_spill;

CREATE TABLE left (k Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right
(
    k Int64,
    p Tuple(x Float64, y Float64, s FixedString(32)),
    t Tuple(a UInt8, b Tuple(c Int64, d Date)),
    n Tuple(Nullable(Int32), UInt8),
    nt Nullable(Tuple(UInt16, Int64)),
    nnt Nullable(Tuple(Nullable(Int32), Int64))
) ENGINE = MergeTree ORDER BY tuple();

-- Left keys 0 to 6 and right keys 10 and 11 have no match.
INSERT INTO left SELECT number FROM numbers(10);
INSERT INTO right SELECT
    number + 7,
    (number, number + 0.5, repeat(toString(number % 10), 32)),
    (number, (-number, toDate('2024-01-01') + number)),
    (if(number = 2, NULL, number), number % 2),
    if(number = 1, NULL, (number, number * 10)),
    if(number = 3, NULL, (if(number = 0, NULL, number), number * 100))
FROM numbers(5);
-- A second match for key 7 and `NULL` at every level.
INSERT INTO right VALUES (7, (9, 9.5, 'nine'), (9, (-9, '2024-02-01')), (NULL, 1), NULL, (NULL, 900));

CREATE TABLE right_spill AS right;
INSERT INTO right_spill SELECT
    number + 7,
    (number, number + 0.5, repeat(toString(number % 10), 32)),
    (number, (-number, toDate('2024-01-01') + number)),
    (if(number = 2, NULL, number), number % 2),
    if(number = 1, NULL, (number, number * 10)),
    if(number % 3 = 0, NULL, (if(number % 4 = 0, NULL, number), number * 100))
FROM numbers(20000);

SET join_algorithm = 'hash';
SET join_use_nulls = 0;

-- Pin planner settings
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 0;
SET min_rows_ratio_for_hash_join_row_store = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

SELECT 'inner', (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_inner';

-- Unmatched rows read back as default tuples and `NULL`.
SELECT 'left', (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l LEFT JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l LEFT JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_left';

SELECT 'full', (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l FULL JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l FULL JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_full';

SELECT 'parallel_hash', (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l LEFT JOIN right r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(toString(tuple(*)))) FROM left l LEFT JOIN right r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_parallel_hash';

-- Switching from `hash` to grace hash join on spilling reads the stored rows back out of the row store.
SELECT 'hash_spilled', (SELECT sum(cityHash64(toString(tuple(*)))) FROM (SELECT number % 20000 + 7 AS k FROM numbers(40000)) l INNER JOIN right_spill r ON l.k = r.k SETTINGS max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1, enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(toString(tuple(*)))) FROM (SELECT number % 20000 + 7 AS k FROM numbers(40000)) l INNER JOIN right_spill r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_hash_spilled';

SYSTEM FLUSH LOGS text_log, query_log;

-- Which queries built a row store.
SELECT replace(log_comment, 'rs_', ''), query_id IN (
    SELECT query_id FROM system.text_log
    WHERE event_date >= yesterday() AND message_format_string = '{}Initialized Row store with {} columns')
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE 'rs\\_%' AND type = 'QueryFinish'
ORDER BY log_comment;

SELECT 'hash_spilled spilled', ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'rs_hash_spilled' AND type = 'QueryFinish';

DROP TABLE right_spill;
DROP TABLE right;
DROP TABLE left;
