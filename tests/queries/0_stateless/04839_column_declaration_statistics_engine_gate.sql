-- The column-declaration spelling `ALTER TABLE ... ADD/MODIFY COLUMN c UInt64 STATISTICS(...)` must honor
-- the same engine gate as the dedicated `ADD/DROP/MODIFY STATISTICS` commands, which non-MergeTree
-- engines reject as NOT_IMPLEMENTED in `checkAlterIsPossible`.

SET allow_statistics = 1;

DROP TABLE IF EXISTS t_stats_gate_memory;
CREATE TABLE t_stats_gate_memory (x UInt64) ENGINE = Memory;
ALTER TABLE t_stats_gate_memory MODIFY COLUMN x UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_stats_gate_memory MODIFY COLUMN x STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_stats_gate_memory ADD COLUMN y UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
-- The dedicated spelling of the same logical alter is rejected the same way.
ALTER TABLE t_stats_gate_memory MODIFY STATISTICS x TYPE tdigest; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_stats_gate_memory;

DROP TABLE IF EXISTS t_stats_gate_dist;
DROP TABLE IF EXISTS t_stats_gate_local;
CREATE TABLE t_stats_gate_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_stats_gate_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't_stats_gate_local');
ALTER TABLE t_stats_gate_dist MODIFY COLUMN x UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_stats_gate_dist ADD COLUMN y UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_stats_gate_dist;

-- Positive control: MergeTree accepts the column-declaration spelling.
ALTER TABLE t_stats_gate_local MODIFY COLUMN x UInt64 STATISTICS(tdigest);
SHOW CREATE TABLE t_stats_gate_local;

-- The gate must follow the target table through an `Alias`: an alias over `MergeTree` supports statistics,
-- so both the dedicated and the column-declaration spelling are accepted through it.
SET allow_experimental_alias_table_engine = 1;
DROP TABLE IF EXISTS t_stats_gate_alias;
CREATE TABLE t_stats_gate_alias ENGINE = Alias('t_stats_gate_local');
ALTER TABLE t_stats_gate_alias MODIFY STATISTICS x TYPE uniq;
ALTER TABLE t_stats_gate_alias MODIFY COLUMN x UInt64 STATISTICS(tdigest);
ALTER TABLE t_stats_gate_alias ADD COLUMN y UInt64 STATISTICS(tdigest);
SHOW CREATE TABLE t_stats_gate_local;
DROP TABLE t_stats_gate_alias;
DROP TABLE t_stats_gate_local;

-- ... and an alias over an engine without statistics support is rejected, like the target itself.
DROP TABLE IF EXISTS t_stats_gate_memory_alias;
CREATE TABLE t_stats_gate_memory (x UInt64) ENGINE = Memory;
CREATE TABLE t_stats_gate_memory_alias ENGINE = Alias('t_stats_gate_memory');
ALTER TABLE t_stats_gate_memory_alias MODIFY COLUMN x UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_stats_gate_memory_alias ADD COLUMN y UInt64 STATISTICS(tdigest); -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_stats_gate_memory_alias;
DROP TABLE t_stats_gate_memory;
