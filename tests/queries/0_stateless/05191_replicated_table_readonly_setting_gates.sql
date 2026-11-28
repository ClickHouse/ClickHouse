-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-ordinary-database
-- no-shared-merge-tree: the checks under test are `ReplicatedMergeTree`'s own; an engine
--   substituted for it accepts `table_readonly` and the refusals below do not happen.
-- no-replicated-database, no-ordinary-database: `ATTACH TABLE ... AS REPLICATED` is supported only
--   for an `Atomic` database.

-- `table_readonly` is not supported for `ReplicatedMergeTree`. A table must not be able to reach
-- that state through any entrypoint - a `CREATE`, an `ALTER` that turns the setting on, or a
-- conversion of a table that carries it - while turning the setting off, which the setting's own
-- documentation promises always works, has to stay allowed: it is the way out for a table whose
-- metadata carries the setting from before these checks existed.

SELECT 'creating a replicated table with the setting is refused';
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

DROP TABLE t_readonly_replicated SYNC;

SELECT 'converting a table that carries the setting is refused';
DROP TABLE IF EXISTS t_readonly_to_convert SYNC;
CREATE TABLE t_readonly_to_convert (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS table_readonly = 1;
DETACH TABLE t_readonly_to_convert;
ATTACH TABLE t_readonly_to_convert AS REPLICATED; -- { serverError NOT_IMPLEMENTED }

SELECT 'and the refusal changed nothing, so the table comes back as it was';
ATTACH TABLE t_readonly_to_convert;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_readonly_to_convert';

SELECT 'once the setting is gone, the conversion goes through';
ALTER TABLE t_readonly_to_convert RESET SETTING table_readonly;
DETACH TABLE t_readonly_to_convert;
ATTACH TABLE t_readonly_to_convert AS REPLICATED;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_readonly_to_convert';

DROP TABLE t_readonly_to_convert SYNC;
