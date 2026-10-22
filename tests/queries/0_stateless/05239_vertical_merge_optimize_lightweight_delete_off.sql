-- `vertical_merge_optimize_lightweight_delete = 0` keeps a merge of parts with lightweight deletes vertical, the deleted
-- rows filtered on read. Test 1: it keeps exactly the rows not deleted. Test 2: a merge by a rows TTL beside a
-- lightweight delete stays vertical too and keeps exactly the rows neither deleted nor expired.
-- `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away.

DROP TABLE IF EXISTS t_lwd_optimize_off;
DROP TABLE IF EXISTS t_lwd_optimize_off_ttl;

-- Test 1: a lightweight delete alone.
CREATE TABLE t_lwd_optimize_off
(
    id UInt64,
    c1 String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    vertical_merge_optimize_lightweight_delete = 0;

INSERT INTO t_lwd_optimize_off SELECT number, toString(number) FROM numbers(15000) WHERE number % 3 = 0;
INSERT INTO t_lwd_optimize_off SELECT number, toString(number) FROM numbers(15000) WHERE number % 3 = 1;
INSERT INTO t_lwd_optimize_off SELECT number, toString(number) FROM numbers(15000) WHERE number % 3 = 2;

DELETE FROM t_lwd_optimize_off WHERE cityHash64(id) % 7 = 0;

OPTIMIZE TABLE t_lwd_optimize_off FINAL;

SELECT 'test1',
    (SELECT count(), sum(cityHash64(id, c1)) FROM t_lwd_optimize_off)
    = (SELECT count(), sum(cityHash64(number, toString(number))) FROM numbers(15000) WHERE cityHash64(number) % 7 != 0);

-- Test 2: a rows TTL and a lightweight delete in one merge, the setting off.
CREATE TABLE t_lwd_optimize_off_ttl
(
    id UInt64,
    d DateTime,
    c1 String
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    vertical_merge_optimize_lightweight_delete = 0;

CREATE VIEW t_lwd_optimize_off_ttl_gen AS
SELECT number AS id, toDateTime(if(cityHash64(number, 1) % 4 = 0, '2000-01-01', '2100-01-01')) AS d, toString(number) AS c1
FROM numbers(15000);

INSERT INTO t_lwd_optimize_off_ttl SELECT * FROM t_lwd_optimize_off_ttl_gen WHERE id % 3 = 0;
INSERT INTO t_lwd_optimize_off_ttl SELECT * FROM t_lwd_optimize_off_ttl_gen WHERE id % 3 = 1;
INSERT INTO t_lwd_optimize_off_ttl SELECT * FROM t_lwd_optimize_off_ttl_gen WHERE id % 3 = 2;

DELETE FROM t_lwd_optimize_off_ttl WHERE cityHash64(id) % 7 = 0;

OPTIMIZE TABLE t_lwd_optimize_off_ttl FINAL;

SELECT 'test2',
    (SELECT count(), sum(cityHash64(id, c1)) FROM t_lwd_optimize_off_ttl)
    = (SELECT count(), sum(cityHash64(id, c1)) FROM t_lwd_optimize_off_ttl_gen WHERE cityHash64(id) % 7 != 0 AND d > now());

SYSTEM FLUSH LOGS part_log;
SELECT 'algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND event_type = 'MergeParts'
    ORDER BY table, event_time_microseconds;

DROP VIEW t_lwd_optimize_off_ttl_gen;
DROP TABLE t_lwd_optimize_off;
DROP TABLE t_lwd_optimize_off_ttl;
