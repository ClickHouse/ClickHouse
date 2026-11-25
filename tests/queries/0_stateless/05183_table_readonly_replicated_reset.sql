-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the test checks how `ReplicatedMergeTree` judges `table_readonly`; with the
-- engine substituted by `SharedMergeTree` the setting is supported.

-- A reset of `table_readonly`, spelled either as `RESET SETTING` or as `MODIFY SETTING table_readonly = DEFAULT`
-- (which is the same command), falls back to the server default. That default is off here, so the reset
-- turns the setting off, which is allowed for `ReplicatedMergeTree`; only turning it on is refused. A server
-- whose `replicated_merge_tree` config section turns it on is covered by the
-- `test_modify_engine_on_restart/test_table_readonly.py` integration test.

DROP TABLE IF EXISTS t_readonly_repl_reset;

CREATE TABLE t_readonly_repl_reset (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl_reset', 'r1') ORDER BY x
SETTINGS merge_with_ttl_timeout = 10;

ALTER TABLE t_readonly_repl_reset RESET SETTING table_readonly;
ALTER TABLE t_readonly_repl_reset MODIFY SETTING table_readonly = DEFAULT;
ALTER TABLE t_readonly_repl_reset MODIFY SETTING merge_with_ttl_timeout = 20, table_readonly = DEFAULT;
ALTER TABLE t_readonly_repl_reset MODIFY SETTING merge_with_ttl_timeout = 30, table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

-- The accepted ALTERs applied what they carried along, the rejected one did not.
SELECT extract(create_table_query, 'merge_with_ttl_timeout = (\\d+)'), create_table_query LIKE '%table_readonly%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_readonly_repl_reset';

DROP TABLE t_readonly_repl_reset;
