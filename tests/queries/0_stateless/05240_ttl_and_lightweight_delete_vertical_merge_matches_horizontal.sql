-- Tests 1-3: a vertical merge that drops rows by lightweight delete and rows TTL returns exactly the rows of a
-- horizontal one, gathered columns included. Keys interleave across the parts, so a gathered row taken from the wrong
-- part changes the result. Test 4: a merge by a rows TTL alone runs `Vertical` and keeps the expected number of rows.
-- `max_bytes_to_merge_at_max_space_in_pool` keeps background merges away.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Test 1: `MergeTree` with a rows TTL, a `WHERE` TTL and lightweight deletes.
DROP TABLE IF EXISTS t_ttl_lwd_ord_v;
DROP TABLE IF EXISTS t_ttl_lwd_ord_h;
DROP VIEW IF EXISTS t_ttl_lwd_ord_gen;

CREATE TABLE t_ttl_lwd_ord_v
(
    id UInt64,
    d DateTime,
    d2 DateTime,
    c1 String,
    c2 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY, d2 + INTERVAL 1 DAY DELETE WHERE c2 % 3 = 0
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- The control: same schema and parts, `vertical_merge_optimize_ttl_delete` off, so it merges horizontally.
CREATE TABLE t_ttl_lwd_ord_h AS t_ttl_lwd_ord_v;
ALTER TABLE t_ttl_lwd_ord_h MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

CREATE VIEW t_ttl_lwd_ord_gen AS
SELECT
    k,
    number * 3 + k AS id,
    toDateTime(if(cityHash64(number, k) % 4 = 0, '2000-01-01', '2100-01-01')) AS d,
    toDateTime(if(cityHash64(number, k, 1) % 2 = 0, '2000-01-01', '2100-01-01')) AS d2,
    concat('p', toString(k), '_', toString(number)) AS c1,
    number * 10 + k AS c2
FROM numbers(5000) ARRAY JOIN [1, 2, 3] AS k;

INSERT INTO t_ttl_lwd_ord_v SELECT * EXCEPT k FROM t_ttl_lwd_ord_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_ord_v SELECT * EXCEPT k FROM t_ttl_lwd_ord_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_ord_v SELECT * EXCEPT k FROM t_ttl_lwd_ord_gen WHERE k = 3;
INSERT INTO t_ttl_lwd_ord_h SELECT * EXCEPT k FROM t_ttl_lwd_ord_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_ord_h SELECT * EXCEPT k FROM t_ttl_lwd_ord_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_ord_h SELECT * EXCEPT k FROM t_ttl_lwd_ord_gen WHERE k = 3;

-- Part 1 loses its head and part 3 a run that spans several blocks; every part loses scattered rows.
DELETE FROM t_ttl_lwd_ord_v WHERE (id % 3 = 1 AND id < 2000) OR (id % 3 = 0 AND id BETWEEN 3000 AND 9000) OR cityHash64(id) % 7 = 0;
DELETE FROM t_ttl_lwd_ord_h WHERE (id % 3 = 1 AND id < 2000) OR (id % 3 = 0 AND id BETWEEN 3000 AND 9000) OR cityHash64(id) % 7 = 0;

OPTIMIZE TABLE t_ttl_lwd_ord_v FINAL;
OPTIMIZE TABLE t_ttl_lwd_ord_h FINAL;

SELECT 'test1_rows', count() FROM t_ttl_lwd_ord_v;
SELECT 'test1_expected_rows', count() FROM t_ttl_lwd_ord_gen
    WHERE NOT ((id % 3 = 1 AND id < 2000) OR (id % 3 = 0 AND id BETWEEN 3000 AND 9000) OR cityHash64(id) % 7 = 0)
        AND d > '2050-01-01' AND NOT (d2 < '2050-01-01' AND c2 % 3 = 0);
SELECT 'test1_same_rows', arraySort(groupArray((id, d, d2, c1, c2)))
    = (SELECT arraySort(groupArray((id, d, d2, c1, c2))) FROM t_ttl_lwd_ord_h)
FROM t_ttl_lwd_ord_v;

SYSTEM FLUSH LOGS part_log;
SELECT 'test1_algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_ttl_lwd_ord_v', 't_ttl_lwd_ord_h') AND event_type = 'MergeParts'
    ORDER BY table;

DROP TABLE t_ttl_lwd_ord_v;
DROP TABLE t_ttl_lwd_ord_h;
DROP VIEW t_ttl_lwd_ord_gen;

-- Test 2: `ReplacingMergeTree`. Every key has three versions in each part. The newest version of every third key
-- is deleted, so the version before it wins; that version is expired for every fifth key, so those keys disappear
-- rather than fall back to an even older version. Other versions are deleted or expired at random.
DROP TABLE IF EXISTS t_ttl_lwd_repl_v;
DROP TABLE IF EXISTS t_ttl_lwd_repl_h;
DROP VIEW IF EXISTS t_ttl_lwd_repl_gen;

CREATE TABLE t_ttl_lwd_repl_v
(
    id UInt64,
    ver UInt64,
    d DateTime,
    d2 DateTime,
    c1 String,
    c2 UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id
TTL d + INTERVAL 1 DAY, d2 + INTERVAL 1 DAY DELETE WHERE c2 % 3 = 0
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE TABLE t_ttl_lwd_repl_h AS t_ttl_lwd_repl_v;
ALTER TABLE t_ttl_lwd_repl_h MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

CREATE VIEW t_ttl_lwd_repl_gen AS
SELECT
    k,
    id,
    ver,
    toDateTime(if((ver = 31 AND id % 5 = 0) OR cityHash64(id, ver) % 11 = 0, '2000-01-01', '2100-01-01')) AS d,
    toDateTime(if(cityHash64(id, ver, 1) % 2 = 0, '2000-01-01', '2100-01-01')) AS d2,
    c1,
    c2
FROM
(
    SELECT
        k,
        number % 1000 AS id,
        k * 10 + intDiv(number, 1000) AS ver,
        concat('p', toString(k), '_', toString(number)) AS c1,
        number * 10 + k AS c2
    FROM numbers(3000) ARRAY JOIN [1, 2, 3] AS k
);

INSERT INTO t_ttl_lwd_repl_v SELECT * EXCEPT k FROM t_ttl_lwd_repl_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_repl_v SELECT * EXCEPT k FROM t_ttl_lwd_repl_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_repl_v SELECT * EXCEPT k FROM t_ttl_lwd_repl_gen WHERE k = 3;
INSERT INTO t_ttl_lwd_repl_h SELECT * EXCEPT k FROM t_ttl_lwd_repl_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_repl_h SELECT * EXCEPT k FROM t_ttl_lwd_repl_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_repl_h SELECT * EXCEPT k FROM t_ttl_lwd_repl_gen WHERE k = 3;

DELETE FROM t_ttl_lwd_repl_v WHERE (ver = 32 AND id % 3 = 0) OR (ver NOT IN (31, 32) AND cityHash64(id, ver) % 5 = 0);
DELETE FROM t_ttl_lwd_repl_h WHERE (ver = 32 AND id % 3 = 0) OR (ver NOT IN (31, 32) AND cityHash64(id, ver) % 5 = 0);

OPTIMIZE TABLE t_ttl_lwd_repl_v FINAL;
OPTIMIZE TABLE t_ttl_lwd_repl_h FINAL;

-- The winner is the newest version that is not deleted; it survives only if the TTL keeps it.
SELECT 'test2_expected_rows', arraySort(groupArray((id, ver))) = (SELECT arraySort(groupArray((id, winner.1))) FROM
    (
        SELECT id, argMax((ver, d, d2, c2), ver) AS winner
        FROM t_ttl_lwd_repl_gen
        WHERE NOT ((ver = 32 AND id % 3 = 0) OR (ver NOT IN (31, 32) AND cityHash64(id, ver) % 5 = 0))
        GROUP BY id
        HAVING winner.2 > '2050-01-01' AND NOT (winner.3 < '2050-01-01' AND winner.4 % 3 = 0)
    ))
FROM t_ttl_lwd_repl_v;
SELECT 'test2_same_rows', arraySort(groupArray((id, ver, d, d2, c1, c2)))
    = (SELECT arraySort(groupArray((id, ver, d, d2, c1, c2))) FROM t_ttl_lwd_repl_h)
FROM t_ttl_lwd_repl_v;

SYSTEM FLUSH LOGS part_log;
SELECT 'test2_algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_ttl_lwd_repl_v', 't_ttl_lwd_repl_h') AND event_type = 'MergeParts'
    ORDER BY table;

DROP TABLE t_ttl_lwd_repl_v;
DROP TABLE t_ttl_lwd_repl_h;
DROP VIEW t_ttl_lwd_repl_gen;

-- Test 3: `MergeTree` where every row has expired and lightweight deletes drop some of them first. The merge
-- emits several blocks and stays vertical. `max_bytes_to_merge_at_max_space_in_pool = 0` also keeps away the
-- background merge that drops a fully expired part.
DROP TABLE IF EXISTS t_ttl_lwd_all_v;
DROP TABLE IF EXISTS t_ttl_lwd_all_h;
DROP VIEW IF EXISTS t_ttl_lwd_all_gen;

CREATE TABLE t_ttl_lwd_all_v
(
    id UInt64,
    d DateTime,
    c1 String,
    c2 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE TABLE t_ttl_lwd_all_h AS t_ttl_lwd_all_v;
ALTER TABLE t_ttl_lwd_all_h MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

CREATE VIEW t_ttl_lwd_all_gen AS
SELECT
    k,
    number * 2 + k AS id,
    toDateTime('2000-01-01') AS d,
    concat('p', toString(k), '_', toString(number)) AS c1,
    number * 10 + k AS c2
FROM numbers(5000) ARRAY JOIN [1, 2] AS k;

INSERT INTO t_ttl_lwd_all_v SELECT * EXCEPT k FROM t_ttl_lwd_all_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_all_v SELECT * EXCEPT k FROM t_ttl_lwd_all_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_all_h SELECT * EXCEPT k FROM t_ttl_lwd_all_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_all_h SELECT * EXCEPT k FROM t_ttl_lwd_all_gen WHERE k = 2;

DELETE FROM t_ttl_lwd_all_v WHERE (id % 2 = 1 AND id < 2000) OR cityHash64(id) % 7 = 0;
DELETE FROM t_ttl_lwd_all_h WHERE (id % 2 = 1 AND id < 2000) OR cityHash64(id) % 7 = 0;

OPTIMIZE TABLE t_ttl_lwd_all_v FINAL;
OPTIMIZE TABLE t_ttl_lwd_all_h FINAL;

SELECT 'test3_rows', (SELECT count() FROM t_ttl_lwd_all_v), (SELECT count() FROM t_ttl_lwd_all_h);

SYSTEM FLUSH LOGS part_log;
SELECT 'test3_algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_ttl_lwd_all_v', 't_ttl_lwd_all_h') AND event_type = 'MergeParts'
    ORDER BY table;

DROP TABLE t_ttl_lwd_all_v;
DROP TABLE t_ttl_lwd_all_h;
DROP VIEW t_ttl_lwd_all_gen;

-- Test 4: a rows TTL alone, on a table whose sorting key column is a `UInt32` named `__ttl_row_position`, the name
-- of the column a vertical TTL merge tags each row with internally.
DROP TABLE IF EXISTS t_ttl_pos_v;
DROP VIEW IF EXISTS t_ttl_pos_gen;

CREATE TABLE t_ttl_pos_v
(
    __ttl_row_position UInt32,
    d DateTime,
    c1 String
)
ENGINE = MergeTree
ORDER BY __ttl_row_position
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE VIEW t_ttl_pos_gen AS
SELECT
    k,
    toUInt32(number * 2 + k) AS __ttl_row_position,
    toDateTime(if(cityHash64(number, k) % 4 = 0, '2000-01-01', '2100-01-01')) AS d,
    concat('p', toString(k), '_', toString(number)) AS c1
FROM numbers(5000) ARRAY JOIN [1, 2] AS k;

INSERT INTO t_ttl_pos_v SELECT * EXCEPT k FROM t_ttl_pos_gen WHERE k = 1;
INSERT INTO t_ttl_pos_v SELECT * EXCEPT k FROM t_ttl_pos_gen WHERE k = 2;

OPTIMIZE TABLE t_ttl_pos_v FINAL;

SELECT 'test4_rows', count() FROM t_ttl_pos_v;
SELECT 'test4_expected_rows', count() FROM t_ttl_pos_gen WHERE d > '2050-01-01';

SYSTEM FLUSH LOGS part_log;
SELECT 'test4_algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_pos_v' AND event_type = 'MergeParts';

DROP TABLE t_ttl_pos_v;
DROP VIEW t_ttl_pos_gen;
