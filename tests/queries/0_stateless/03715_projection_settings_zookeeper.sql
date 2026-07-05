-- Tags: long, zookeeper, no-random-merge-tree-settings, no-replicated-database
-- { echo ON }

DROP TABLE IF EXISTS x1;
DROP TABLE IF EXISTS x2;

CREATE TABLE x1 (i int) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r1') ORDER BY i SETTINGS index_granularity = 999999999, index_granularity_bytes = 99999999999, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0;

CREATE TABLE x2 (i int) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r2') ORDER BY i SETTINGS index_granularity = 999999999, index_granularity_bytes = 99999999999, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0;

ALTER TABLE x1 ADD PROJECTION p1 (SELECT i ORDER BY i) WITH SETTINGS (index_granularity = 2, index_granularity_bytes = 999999999);
ALTER TABLE x1 ADD PROJECTION p2 (SELECT i ORDER BY i) WITH SETTINGS (index_granularity = 9999999999, index_granularity_bytes = 4096);

INSERT INTO x1 SELECT number FROM numbers(1000);

OPTIMIZE TABLE x1 FINAL;

SYSTEM SYNC REPLICA x2;

SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'x1' AND name = 'p1';
SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'x1' AND name = 'p2';

DETACH TABLE x2 SYNC;
ATTACH TABLE x2;

SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'x2' AND name = 'p1';
SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'x2' AND name = 'p2';

DROP TABLE x1;
DROP TABLE x2;
