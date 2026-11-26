-- Tests 1-2: a vertical merge of a collapsing table dropping rows by lightweight delete and rows TTL returns the rows
-- of a horizontal one, gathered columns included; keys interleave across parts, so a row gathered from the wrong part
-- changes the result.
-- `max_bytes_to_merge_at_max_space_in_pool` keeps background merges away.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Test 1: `CollapsingMergeTree`. Every key has one row in each part, with a random sign. Lightweight deletes drop
-- the -1 row of some keys and the +1 rows of others, so their cancel pairs lose one side; the TTLs expire rows of
-- both signs at random.
DROP TABLE IF EXISTS t_ttl_lwd_coll_v;
DROP TABLE IF EXISTS t_ttl_lwd_coll_h;
DROP VIEW IF EXISTS t_ttl_lwd_coll_gen;

CREATE TABLE t_ttl_lwd_coll_v
(
    id UInt64,
    sign Int8,
    d DateTime,
    d2 DateTime,
    c1 String,
    c2 UInt64
)
ENGINE = CollapsingMergeTree(sign)
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

CREATE TABLE t_ttl_lwd_coll_h AS t_ttl_lwd_coll_v;
ALTER TABLE t_ttl_lwd_coll_h MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

CREATE VIEW t_ttl_lwd_coll_gen AS
SELECT
    k,
    number AS id,
    toInt8(if(cityHash64(number, k, 2) % 2 = 0, 1, -1)) AS sign,
    toDateTime(if(cityHash64(number, k) % 4 = 0, '2000-01-01', '2100-01-01')) AS d,
    toDateTime(if(cityHash64(number, k, 1) % 2 = 0, '2000-01-01', '2100-01-01')) AS d2,
    concat('p', toString(k), '_', toString(number)) AS c1,
    number * 10 + k AS c2
FROM numbers(3000) ARRAY JOIN [1, 2, 3] AS k;

INSERT INTO t_ttl_lwd_coll_v SELECT * EXCEPT k FROM t_ttl_lwd_coll_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_coll_v SELECT * EXCEPT k FROM t_ttl_lwd_coll_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_coll_v SELECT * EXCEPT k FROM t_ttl_lwd_coll_gen WHERE k = 3;
INSERT INTO t_ttl_lwd_coll_h SELECT * EXCEPT k FROM t_ttl_lwd_coll_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_coll_h SELECT * EXCEPT k FROM t_ttl_lwd_coll_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_coll_h SELECT * EXCEPT k FROM t_ttl_lwd_coll_gen WHERE k = 3;

DELETE FROM t_ttl_lwd_coll_v WHERE (sign = -1 AND id % 3 = 0) OR (sign = 1 AND id % 5 = 0) OR cityHash64(c2) % 7 = 0;
DELETE FROM t_ttl_lwd_coll_h WHERE (sign = -1 AND id % 3 = 0) OR (sign = 1 AND id % 5 = 0) OR cityHash64(c2) % 7 = 0;

OPTIMIZE TABLE t_ttl_lwd_coll_v FINAL;
OPTIMIZE TABLE t_ttl_lwd_coll_h FINAL;

SELECT 'test1_rows', (SELECT count() FROM t_ttl_lwd_coll_v), (SELECT countIf(sign = -1) FROM t_ttl_lwd_coll_v);
SELECT 'test1_same_rows', arraySort(groupArray((id, sign, d, d2, c1, c2)))
    = (SELECT arraySort(groupArray((id, sign, d, d2, c1, c2))) FROM t_ttl_lwd_coll_h)
FROM t_ttl_lwd_coll_v;

SYSTEM FLUSH LOGS part_log;
SELECT 'test1_algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_ttl_lwd_coll_v', 't_ttl_lwd_coll_h') AND event_type = 'MergeParts'
    ORDER BY table;

DROP TABLE t_ttl_lwd_coll_v;
DROP TABLE t_ttl_lwd_coll_h;
DROP VIEW t_ttl_lwd_coll_gen;

-- Test 2: `VersionedCollapsingMergeTree`. Every key has a state in part 1, its cancel row in part 2 and a new state
-- in part 3; for every fourth key part 2 cancels the new state instead. Lightweight deletes drop the cancel row of
-- some keys and states of others; the TTLs expire rows of both signs at random.
DROP TABLE IF EXISTS t_ttl_lwd_vcoll_v;
DROP TABLE IF EXISTS t_ttl_lwd_vcoll_h;
DROP VIEW IF EXISTS t_ttl_lwd_vcoll_gen;

CREATE TABLE t_ttl_lwd_vcoll_v
(
    id UInt64,
    ver UInt64,
    sign Int8,
    d DateTime,
    d2 DateTime,
    c1 String,
    c2 UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, ver)
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

CREATE TABLE t_ttl_lwd_vcoll_h AS t_ttl_lwd_vcoll_v;
ALTER TABLE t_ttl_lwd_vcoll_h MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

CREATE VIEW t_ttl_lwd_vcoll_gen AS
SELECT
    k,
    number AS id,
    toUInt64(multiIf(k = 1, 1, k = 2 AND number % 4 != 0, 1, 2)) AS ver,
    toInt8(if(k = 2, -1, 1)) AS sign,
    toDateTime(if(cityHash64(number, k) % 4 = 0, '2000-01-01', '2100-01-01')) AS d,
    toDateTime(if(cityHash64(number, k, 1) % 2 = 0, '2000-01-01', '2100-01-01')) AS d2,
    concat('p', toString(k), '_', toString(number)) AS c1,
    number * 10 + k AS c2
FROM numbers(3000) ARRAY JOIN [1, 2, 3] AS k;

INSERT INTO t_ttl_lwd_vcoll_v SELECT * EXCEPT k FROM t_ttl_lwd_vcoll_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_vcoll_v SELECT * EXCEPT k FROM t_ttl_lwd_vcoll_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_vcoll_v SELECT * EXCEPT k FROM t_ttl_lwd_vcoll_gen WHERE k = 3;
INSERT INTO t_ttl_lwd_vcoll_h SELECT * EXCEPT k FROM t_ttl_lwd_vcoll_gen WHERE k = 1;
INSERT INTO t_ttl_lwd_vcoll_h SELECT * EXCEPT k FROM t_ttl_lwd_vcoll_gen WHERE k = 2;
INSERT INTO t_ttl_lwd_vcoll_h SELECT * EXCEPT k FROM t_ttl_lwd_vcoll_gen WHERE k = 3;

DELETE FROM t_ttl_lwd_vcoll_v WHERE (sign = -1 AND id % 3 = 0) OR (sign = 1 AND cityHash64(c2) % 5 = 0);
DELETE FROM t_ttl_lwd_vcoll_h WHERE (sign = -1 AND id % 3 = 0) OR (sign = 1 AND cityHash64(c2) % 5 = 0);

OPTIMIZE TABLE t_ttl_lwd_vcoll_v FINAL;
OPTIMIZE TABLE t_ttl_lwd_vcoll_h FINAL;

SELECT 'test2_rows', (SELECT count() FROM t_ttl_lwd_vcoll_v), (SELECT countIf(sign = -1) FROM t_ttl_lwd_vcoll_v);
SELECT 'test2_same_rows', arraySort(groupArray((id, ver, sign, d, d2, c1, c2)))
    = (SELECT arraySort(groupArray((id, ver, sign, d, d2, c1, c2))) FROM t_ttl_lwd_vcoll_h)
FROM t_ttl_lwd_vcoll_v;

SYSTEM FLUSH LOGS part_log;
SELECT 'test2_algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_ttl_lwd_vcoll_v', 't_ttl_lwd_vcoll_h') AND event_type = 'MergeParts'
    ORDER BY table;

DROP TABLE t_ttl_lwd_vcoll_v;
DROP TABLE t_ttl_lwd_vcoll_h;
DROP VIEW t_ttl_lwd_vcoll_gen;
