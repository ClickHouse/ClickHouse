-- index_granularity and index_granularity_bytes are alterable on a non-replicated, fully adaptive
-- MergeTree: the change only sets the write-time granule size of newly written parts, while existing
-- parts keep reading their own granularity from their marks. They stay readonly when the change would
-- switch the table away from adaptive granularity, and on ReplicatedMergeTree.

DROP TABLE IF EXISTS t_alter_granularity;

CREATE TABLE t_alter_granularity (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

-- Keep the two inserted parts separate so their mark counts stay comparable.
SYSTEM STOP MERGES t_alter_granularity;

-- First part written with granularity 8192.
INSERT INTO t_alter_granularity SELECT number FROM numbers(20000);

-- index_granularity is alterable on a non-replicated adaptive table.
ALTER TABLE t_alter_granularity MODIFY SETTING index_granularity = 1024;

-- The existing part is still read correctly (it carries its own granularity in its marks).
SELECT 'read_after_alter', count(), sum(a) FROM t_alter_granularity;

-- The new part is written with the new, smaller granularity, so the two active parts differ in marks.
INSERT INTO t_alter_granularity SELECT number FROM numbers(20000, 20000);
SELECT 'distinct_marks_per_part', count(DISTINCT marks) FROM system.parts
WHERE database = currentDatabase() AND table = 't_alter_granularity' AND active;

SELECT 'total', count(), sum(a) FROM t_alter_granularity;

-- index_granularity_bytes can change to another non-zero value (table stays adaptive).
ALTER TABLE t_alter_granularity MODIFY SETTING index_granularity_bytes = 1000000;

-- RESET reverts to the (non-zero) defaults and is allowed too. index_granularity stays stated in the
-- definition with its default, so resetting every setting still leaves a valid SETTINGS clause.
ALTER TABLE t_alter_granularity RESET SETTING index_granularity;
ALTER TABLE t_alter_granularity RESET SETTING index_granularity_bytes;
SELECT 'granularity_stated_after_reset', position(create_table_query, 'index_granularity = ') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_granularity';

-- The stored definition loads back.
DETACH TABLE t_alter_granularity;
ATTACH TABLE t_alter_granularity;
SELECT 'total_after_attach', count(), sum(a) FROM t_alter_granularity;

-- Switching the table to fixed (non-adaptive) granularity is rejected: it would change the on-disk
-- mark format of new parts.
ALTER TABLE t_alter_granularity MODIFY SETTING index_granularity_bytes = 0; -- { serverError READONLY_SETTING }

DROP TABLE t_alter_granularity;

-- On a non-adaptive table (index_granularity_bytes = 0) index_granularity is load-bearing on read,
-- so it stays readonly.
DROP TABLE IF EXISTS t_alter_granularity_fixed;
CREATE TABLE t_alter_granularity_fixed (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
ALTER TABLE t_alter_granularity_fixed MODIFY SETTING index_granularity = 4096; -- { serverError READONLY_SETTING }

-- A non-adaptive part brought into an adaptive table is read with that table's index_granularity, so
-- the setting stays readonly while the part is there, attached or detached.
DROP TABLE IF EXISTS t_alter_granularity_import;
CREATE TABLE t_alter_granularity_import (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO t_alter_granularity_fixed SELECT number FROM numbers(20000);
ALTER TABLE t_alter_granularity_import ATTACH PARTITION ID 'all' FROM t_alter_granularity_fixed;
ALTER TABLE t_alter_granularity_import MODIFY SETTING index_granularity = 1024; -- { serverError READONLY_SETTING }

-- The reload leaves the part only in the detached directory.
ALTER TABLE t_alter_granularity_import DETACH PARTITION ID 'all';
DETACH TABLE t_alter_granularity_import;
ATTACH TABLE t_alter_granularity_import;
ALTER TABLE t_alter_granularity_import MODIFY SETTING index_granularity = 1024; -- { serverError READONLY_SETTING }

-- Without the part the change is allowed, and a non-adaptive part of the old granularity is not accepted anymore.
ALTER TABLE t_alter_granularity_import DROP DETACHED PARTITION ID 'all' SETTINGS allow_drop_detached = 1;
ALTER TABLE t_alter_granularity_import MODIFY SETTING index_granularity = 1024;
ALTER TABLE t_alter_granularity_import ATTACH PARTITION ID 'all' FROM t_alter_granularity_fixed; -- { serverError BAD_ARGUMENTS }
SELECT 'import_after_change', count() FROM t_alter_granularity_import;

DROP TABLE t_alter_granularity_import;
DROP TABLE t_alter_granularity_fixed;

-- On ReplicatedMergeTree both settings stay readonly: they are immutable table-identity fields stored
-- in Keeper that every replica must match. A non-default index_granularity makes RESET an actual change.
DROP TABLE IF EXISTS t_alter_granularity_repl;
CREATE TABLE t_alter_granularity_repl (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_alter_granularity_repl', 'r1') ORDER BY a
SETTINGS index_granularity = 4096;
ALTER TABLE t_alter_granularity_repl MODIFY SETTING index_granularity = 2048; -- { serverError READONLY_SETTING }
ALTER TABLE t_alter_granularity_repl MODIFY SETTING index_granularity_bytes = 1000000; -- { serverError READONLY_SETTING }
ALTER TABLE t_alter_granularity_repl RESET SETTING index_granularity; -- { serverError READONLY_SETTING }
DROP TABLE t_alter_granularity_repl;
