-- Tags: long, zookeeper, no-random-merge-tree-settings, no-replicated-database
-- { echo ON }

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS x1;
DROP TABLE IF EXISTS x2;

CREATE TABLE x1 (i String, projection p index i type text(tokenizer = array)) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r1') ORDER BY i SETTINGS index_granularity = 999999999, index_granularity_bytes = 99999999999, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0;

CREATE TABLE x2 (i String, projection p index i type text(tokenizer = array)) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r2') ORDER BY i SETTINGS index_granularity = 999999999, index_granularity_bytes = 99999999999, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0;

INSERT INTO x1 SELECT number % 10 FROM numbers(1000);

OPTIMIZE TABLE x1 FINAL;

SYSTEM SYNC REPLICA x2;

DETACH TABLE x2 SYNC;
ATTACH TABLE x2;

SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'x2' AND name = 'p1';
SELECT marks FROM system.projection_parts WHERE active AND database = currentDatabase() AND table = 'x2' AND name = 'p2';

DROP TABLE x1;
DROP TABLE x2;
