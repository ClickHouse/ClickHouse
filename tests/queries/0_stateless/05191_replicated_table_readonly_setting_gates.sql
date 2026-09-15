-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the checks under test are `ReplicatedMergeTree`'s own; an engine
--   substituted for it accepts `table_readonly` and the refusals below do not happen.

-- `table_readonly` is not supported for `ReplicatedMergeTree`, and the checks that say so used to
-- trap a table whose metadata carries it anyway (a table converted to replicated by the
-- `convert_to_replicated` flag): a backup of that working table could not be restored, a detached
-- table could not be re-attached, and `ALTER TABLE ... MODIFY SETTING table_readonly = 0` - which the
-- setting's own documentation promises always works - was refused as well.

SELECT 'creating a replicated table with the setting is still refused';
DROP TABLE IF EXISTS t_readonly_replicated SYNC;
CREATE TABLE t_readonly_replicated (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_replicated', 'r1')
ORDER BY id SETTINGS table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'and so is turning it on later';
CREATE TABLE t_readonly_replicated (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_replicated', 'r1')
ORDER BY id;
INSERT INTO t_readonly_replicated SELECT number FROM numbers(10);
ALTER TABLE t_readonly_replicated MODIFY SETTING table_readonly = 1; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_readonly_replicated MODIFY COLUMN id UInt64 CODEC(NONE), MODIFY SETTING table_readonly = 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'turning it off is allowed, whatever it was';
ALTER TABLE t_readonly_replicated MODIFY SETTING table_readonly = 0;
ALTER TABLE t_readonly_replicated RESET SETTING table_readonly;
SELECT count() FROM t_readonly_replicated;

SELECT 'a table whose metadata carries the setting can be attached';
DETACH TABLE t_readonly_replicated;
ATTACH TABLE t_readonly_replicated;
SELECT count() FROM t_readonly_replicated;

DROP TABLE t_readonly_replicated SYNC;
