SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_shared;

CREATE TABLE t_shared (tier String, ts DateTime('UTC'))
ENGINE = MergeTree ORDER BY tier
TTL ts + INTERVAL 1 MONTH DELETE WHERE tier = 'a',
    ts + INTERVAL 1 MONTH DELETE WHERE tier = 'b',
    ts + INTERVAL 1 MONTH DELETE WHERE tier = 'c'
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- Rule 'a' keeps a live row, rules 'b' and 'c' keep none, and all three
-- rules share one rows_where_ttl slot keyed by the time expression.
INSERT INTO t_shared VALUES ('a', '2020-01-01 00:00:00'), ('a', '2099-01-01 00:00:00'), ('b', '2020-01-01 00:00:00'), ('c', '2020-01-01 00:00:00');

OPTIMIZE TABLE t_shared FINAL;

-- One slot for three rules, and it must carry the live 'a' row rather than the empty
-- info of the last rule finalized.
SELECT 'merged', length(rows_where_ttl_info.expression), rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active;

SELECT 'rows', tier, ts FROM t_shared ORDER BY ALL;

-- The TTL a future merge is scheduled from is recomputed on load, so it must survive a restart.
DETACH TABLE t_shared;
ATTACH TABLE t_shared;

SELECT 'reloaded', rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active;

ALTER TABLE t_shared MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT 'materialized', rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active;

DROP TABLE t_shared;

DROP TABLE IF EXISTS t_reordered;

-- The live rule is declared in the middle, so neither the first nor the last rule
-- finalized carries the range: the slot must not depend on declaration order.
CREATE TABLE t_reordered (tier String, ts DateTime('UTC'))
ENGINE = MergeTree ORDER BY tier
TTL ts + INTERVAL 1 MONTH DELETE WHERE tier = 'b',
    ts + INTERVAL 1 MONTH DELETE WHERE tier = 'a',
    ts + INTERVAL 1 MONTH DELETE WHERE tier = 'c'
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_reordered VALUES ('a', '2020-01-01 00:00:00'), ('a', '2099-01-01 00:00:00'), ('b', '2020-01-01 00:00:00'), ('c', '2020-01-01 00:00:00');

OPTIMIZE TABLE t_reordered FINAL;

SELECT 'reordered', rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 't_reordered' AND active;

DROP TABLE t_reordered;

DROP TABLE IF EXISTS t_two_live;

-- Two rules each keep a live row, so the shared slot must span both: min from 'a', max from 'b'.
-- Any form of assignment would keep only one rule's range.
CREATE TABLE t_two_live (tier String, ts DateTime('UTC'))
ENGINE = MergeTree ORDER BY tier
TTL ts + INTERVAL 1 MONTH DELETE WHERE tier = 'a',
    ts + INTERVAL 1 MONTH DELETE WHERE tier = 'b'
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_two_live VALUES ('a', '2020-01-01 00:00:00'), ('a', '2099-01-01 00:00:00'), ('b', '2020-01-01 00:00:00'), ('b', '2100-01-01 00:00:00');

OPTIMIZE TABLE t_two_live FINAL;

SELECT 'two live', rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 't_two_live' AND active;

SELECT 'two live rows', tier, ts FROM t_two_live ORDER BY ALL;

DROP TABLE t_two_live;

DROP TABLE IF EXISTS t_distinct;

-- Control: distinct time expressions get distinct slots, so nothing is shared.
CREATE TABLE t_distinct (tier String, ts DateTime('UTC'))
ENGINE = MergeTree ORDER BY tier
TTL ts + INTERVAL 1 MONTH DELETE WHERE tier = 'a',
    ts + INTERVAL 2 MONTH DELETE WHERE tier = 'b'
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_distinct VALUES ('a', '2020-01-01 00:00:00'), ('a', '2099-01-01 00:00:00'), ('b', '2020-01-01 00:00:00');

OPTIMIZE TABLE t_distinct FINAL;

SELECT 'control', rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts WHERE database = currentDatabase() AND table = 't_distinct' AND active;

SELECT 'control rows', tier, ts FROM t_distinct ORDER BY ALL;

DROP TABLE t_distinct;
