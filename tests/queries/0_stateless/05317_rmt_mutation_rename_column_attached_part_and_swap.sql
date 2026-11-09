-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and SYSTEM SYNC REPLICA ordering of a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue
-- A mutation must keep the data when it renames the columns of an attached part, or when it is queued before a swap of two column names.

-- ALTER UPDATE of another column on a part attached with the pre-rename column names.
DROP TABLE IF EXISTS t_attach SYNC;
CREATE TABLE t_attach (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_attach', '1') ORDER BY tuple() PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_attach VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_attach DETACH PARTITION tuple();
ALTER TABLE t_attach RENAME COLUMN b TO c SETTINGS alter_sync = 2;
ALTER TABLE t_attach ATTACH PARTITION tuple();
SELECT 'attach before', id, a, c FROM t_attach ORDER BY id;
ALTER TABLE t_attach UPDATE a = a + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'attach', id, a, c FROM t_attach ORDER BY id;
CHECK TABLE t_attach SETTINGS check_query_single_value_result = 1;
DROP TABLE t_attach;

-- ALTER UPDATE of the renamed column itself on a part attached with the pre-rename column names.
DROP TABLE IF EXISTS t_attach_target SYNC;
CREATE TABLE t_attach_target (id UInt32, a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_attach_target', '1') ORDER BY tuple() PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_attach_target VALUES (1, 1111, 2222), (2, 3333, 4444);
ALTER TABLE t_attach_target DETACH PARTITION tuple();
ALTER TABLE t_attach_target RENAME COLUMN b TO c SETTINGS alter_sync = 2;
ALTER TABLE t_attach_target ATTACH PARTITION tuple();
ALTER TABLE t_attach_target UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'attach target', id, a, c FROM t_attach_target ORDER BY id;
CHECK TABLE t_attach_target SETTINGS check_query_single_value_result = 1;
DROP TABLE t_attach_target;

-- ALTER UPDATE queued before a swap of two column names, wide and compact parts.
DROP TABLE IF EXISTS t_swap_wide SYNC;
CREATE TABLE t_swap_wide (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_swap_wide', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_swap_wide VALUES (1, 10, 11, 100), (2, 20, 21, 200);
ALTER TABLE t_swap_wide UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_swap_wide RENAME COLUMN a TO b, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_swap_wide;
SELECT 'swap wide pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_swap_wide' AND NOT is_done;
ALTER TABLE t_swap_wide MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_swap_wide UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'swap wide', id, x, b, a FROM t_swap_wide ORDER BY id;
CHECK TABLE t_swap_wide SETTINGS check_query_single_value_result = 1;
DROP TABLE t_swap_wide;

DROP TABLE IF EXISTS t_swap_compact SYNC;
CREATE TABLE t_swap_compact (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_swap_compact', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_swap_compact VALUES (1, 10, 11, 100), (2, 20, 21, 200);
ALTER TABLE t_swap_compact UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_swap_compact RENAME COLUMN a TO b, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_swap_compact;
ALTER TABLE t_swap_compact MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_swap_compact UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'swap compact', id, x, b, a FROM t_swap_compact ORDER BY id;
DROP TABLE t_swap_compact;

-- The swapped column is rewritten by the queued ALTER UPDATE; t_swap_shared shares the source part's files.
DROP TABLE IF EXISTS t_swap_rewrite SYNC;
DROP TABLE IF EXISTS t_swap_shared SYNC;
CREATE TABLE t_swap_rewrite (id UInt32, x UInt32, a UInt32 MATERIALIZED x + 1, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_swap_rewrite', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;
CREATE TABLE t_swap_shared (id UInt32, x UInt32, a UInt32 MATERIALIZED x + 1, c UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_swap_rewrite (id, x, c) VALUES (1, 10, 100), (2, 20, 200);
ALTER TABLE t_swap_shared ATTACH PARTITION tuple() FROM t_swap_rewrite;
ALTER TABLE t_swap_rewrite UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_swap_rewrite RENAME COLUMN a TO b, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_swap_rewrite;
ALTER TABLE t_swap_rewrite MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_swap_rewrite UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'swap rewrite', id, x, b, a FROM t_swap_rewrite ORDER BY id;
SELECT 'swap shared', id, x, a, c FROM t_swap_shared ORDER BY id;
CHECK TABLE t_swap_shared SETTINGS check_query_single_value_result = 1;
DROP TABLE t_swap_rewrite;
DROP TABLE t_swap_shared;
