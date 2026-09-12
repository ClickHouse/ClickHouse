-- Tags: zookeeper

DROP TABLE IF EXISTS t_minmax_count_alter_r1;
DROP TABLE IF EXISTS t_minmax_count_alter_r2;

SET optimize_use_projections = 1;

CREATE TABLE t_minmax_count_alter_r1 (carrier UInt64, value UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/minmax_count_alter', 'r1') ORDER BY tuple();
CREATE TABLE t_minmax_count_alter_r2 (carrier UInt64, value UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/minmax_count_alter', 'r2') ORDER BY tuple();

INSERT INTO t_minmax_count_alter_r1 VALUES (1, 10), (2, 20);
SYSTEM SYNC REPLICA t_minmax_count_alter_r2;

ALTER TABLE t_minmax_count_alter_r1 RENAME COLUMN carrier TO renamed;
SYSTEM SYNC REPLICA t_minmax_count_alter_r2;

SELECT
    (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter_r2 SETTINGS optimize_use_implicit_projections = 1)
    = (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter_r2 SETTINGS optimize_use_implicit_projections = 0);
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM t_minmax_count_alter_r2 SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 1)
WHERE explain ILIKE '%_minmax_count_projection%';

ALTER TABLE t_minmax_count_alter_r1 DROP COLUMN renamed;
SYSTEM SYNC REPLICA t_minmax_count_alter_r2;

SELECT
    (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter_r2 SETTINGS optimize_use_implicit_projections = 1)
    = (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter_r2 SETTINGS optimize_use_implicit_projections = 0);
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM t_minmax_count_alter_r2 SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 1)
WHERE explain ILIKE '%_minmax_count_projection%';

DROP TABLE t_minmax_count_alter_r1;
DROP TABLE t_minmax_count_alter_r2;
