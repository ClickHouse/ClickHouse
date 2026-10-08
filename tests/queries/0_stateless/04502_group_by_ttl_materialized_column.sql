-- Rows rolled up by `GROUP BY` TTL get `any` values of MATERIALIZED columns
-- instead of an exception during the merge or the mutation.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_group_by_materialized SYNC;

CREATE TABLE t_ttl_group_by_materialized (v1 DateTime64(3), v2 Int16, m UInt64 MATERIALIZED v2 * 2)
ENGINE = MergeTree ORDER BY v1
TTL toStartOfDay(v1) + INTERVAL 1 DAY GROUP BY v1
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_ttl_group_by_materialized;
INSERT INTO t_ttl_group_by_materialized (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);
INSERT INTO t_ttl_group_by_materialized (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);
INSERT INTO t_ttl_group_by_materialized (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);
INSERT INTO t_ttl_group_by_materialized (v1, v2) VALUES (toDateTime('2001-09-18 10:03:30'), 17349);

SELECT 'before';
SELECT m, * FROM t_ttl_group_by_materialized ORDER BY ALL;

SYSTEM START TTL MERGES t_ttl_group_by_materialized;
OPTIMIZE TABLE t_ttl_group_by_materialized FINAL;

SELECT 'after merge';
SELECT m, * FROM t_ttl_group_by_materialized ORDER BY ALL;

ALTER TABLE t_ttl_group_by_materialized REWRITE PARTS;

SELECT 'after rewrite';
SELECT m, * FROM t_ttl_group_by_materialized ORDER BY ALL;

DROP TABLE t_ttl_group_by_materialized;
