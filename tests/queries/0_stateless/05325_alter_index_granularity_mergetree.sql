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

-- RESET reverts to the (non-zero) defaults and is allowed too.
ALTER TABLE t_alter_granularity RESET SETTING index_granularity;
ALTER TABLE t_alter_granularity RESET SETTING index_granularity_bytes;

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
DROP TABLE t_alter_granularity_fixed;

-- On ReplicatedMergeTree both settings stay readonly: they are immutable table-identity fields stored
-- in Keeper that every replica must match.
DROP TABLE IF EXISTS t_alter_granularity_repl;
CREATE TABLE t_alter_granularity_repl (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_alter_granularity_repl', 'r1') ORDER BY a;
ALTER TABLE t_alter_granularity_repl MODIFY SETTING index_granularity = 4096; -- { serverError READONLY_SETTING }
ALTER TABLE t_alter_granularity_repl MODIFY SETTING index_granularity_bytes = 1000000; -- { serverError READONLY_SETTING }
ALTER TABLE t_alter_granularity_repl RESET SETTING index_granularity; -- { serverError READONLY_SETTING }
DROP TABLE t_alter_granularity_repl;
