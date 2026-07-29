-- Tags: no-fasttest
-- Test: DETACH TABLE ... PERMANENTLY honours `check_referential_table_dependencies=1`
-- and leaves the source table fully usable after the failed dependency check.
DROP TABLE IF EXISTS mv_ref;
DROP TABLE IF EXISTS dst_ref;
DROP TABLE IF EXISTS src_ref;

CREATE TABLE src_ref (x UInt8, v UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_ref (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_ref TO dst_ref AS SELECT x FROM src_ref;
INSERT INTO src_ref VALUES (1, 0), (2, 0), (3, 0);

SET check_referential_table_dependencies = 1;

-- Referential dependency must block the permanent detach.
DETACH TABLE src_ref PERMANENTLY; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- After the failed detach the source table must stay in the catalog and readable.
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'src_ref';
SELECT count() FROM src_ref;

-- Regression guard for #105259: the dependency check must run BEFORE the storage
-- is shut down. A plain SELECT cannot detect a stray flushAndShutdown because
-- StorageMergeTree::read never consults shutdown_called; mutations do. A shut-down
-- MergeTree silently discards mutations (scheduleDataProcessingJob bails on
-- shutdown_called and waitForMutation returns "done" immediately), so a synchronous
-- mutation that actually changes the data proves background work is still live.
ALTER TABLE src_ref UPDATE v = v + 10 WHERE 1 SETTINGS mutations_sync = 2;
SELECT sum(v) FROM src_ref;

DROP TABLE mv_ref;
DROP TABLE src_ref;
DROP TABLE dst_ref;
