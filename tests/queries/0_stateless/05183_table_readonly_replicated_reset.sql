-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the test checks that `table_readonly` is rejected specifically for
-- `ReplicatedMergeTree`; with the engine substituted by `SharedMergeTree` the ALTERs succeed.

-- Resetting `table_readonly` must be rejected as well, either spelled as `RESET SETTING` or as
-- `MODIFY SETTING table_readonly = DEFAULT`, which is the same command.

DROP TABLE IF EXISTS t_readonly_repl_reset;

CREATE TABLE t_readonly_repl_reset (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl_reset', 'r1') ORDER BY x
SETTINGS merge_with_ttl_timeout = 10;

ALTER TABLE t_readonly_repl_reset RESET SETTING table_readonly; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_readonly_repl_reset MODIFY SETTING table_readonly = DEFAULT; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_readonly_repl_reset MODIFY SETTING merge_with_ttl_timeout = 20, table_readonly = DEFAULT; -- { serverError NOT_IMPLEMENTED }

-- The rejected ALTER must not have applied the settings it carried along.
SELECT extract(create_table_query, 'merge_with_ttl_timeout = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_readonly_repl_reset';

DROP TABLE t_readonly_repl_reset;
