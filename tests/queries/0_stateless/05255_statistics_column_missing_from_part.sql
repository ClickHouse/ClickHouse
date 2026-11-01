-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN checks below read plan nodes that parallel replicas replaces with a remote read step.

-- A column statistic describes a column of a data part, so a mutation must not write one for a column
-- the part it produces does not store. A metadata-only ALTER ADD COLUMN leaves a wide part without the
-- column, and a read of it is then synthesized from the current DEFAULT, so a statistic frozen for it
-- stops describing what a read returns as soon as that DEFAULT changes. Check that min/max aggregation
-- and statistics part pruning keep agreeing with a plain read, for a mutation that carries the part's
-- columns over and for one that rewrites them all, and that a column the part does store keeps both.

SET mutations_sync = 2, alter_sync = 2;
SET use_statistics_for_part_pruning = 1, use_statistics_for_min_max_aggregation = 1;
-- An insert must not be allowed to supply a statistic that a mutation below is supposed to supply, or
-- a check on what a mutation wrote would also hold on a server where the mutation did nothing.
SET materialize_statistics_on_insert = 0;
-- The min/max shortcut lives in the aggregate-projection pass, so anything that disables that pass
-- also disables the shortcut and would make the min/max checks pass on a build that has the bug.
-- The runner randomizes all of these except force_aggregation_in_order and
-- aggregate_functions_null_for_empty, which are the pass's remaining eligibility conditions.
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0, aggregate_functions_null_for_empty = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_carried_over;
DROP TABLE IF EXISTS t_rewritten;
DROP TABLE IF EXISTS t_stored;

-- The test runner randomizes every MergeTree setting a table does not set itself, so spell out the
-- ones the fixture needs: a wide part is what leaves a late-added column unmaterialized, the block
-- columns decide which mutation path runs, and the statistics are declared per column so the result
-- does not depend on the randomized `auto_statistics_types`.
-- `s` is stored by the part and `v` is not, and one mutation materializes the statistics of both, so
-- the same mutation that must skip `v` has to keep `s`. That makes the two checks below a pair: `s`
-- having a statistic is only possible if the mutation ran, which is what lets the absence of one for
-- `v` mean the mutation declined it rather than that nothing happened.
CREATE TABLE t_carried_over (id UInt64, s UInt32 STATISTICS(basic)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         auto_statistics_types = '', enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_carried_over SELECT number, 10 FROM numbers(4);
ALTER TABLE t_carried_over ADD COLUMN v UInt32 DEFAULT 7 STATISTICS(basic);
ALTER TABLE t_carried_over MATERIALIZE STATISTICS v, s;
ALTER TABLE t_carried_over MODIFY COLUMN v UInt32 DEFAULT 99;

SELECT 'a mutation that carries the columns over';
-- the premise: the part has no bytes for `v`
SELECT arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_carried_over' AND active;
SELECT arraySort(groupArray(v)) FROM t_carried_over;
SELECT max(v), min(v) FROM t_carried_over;
SELECT count() FROM t_carried_over WHERE v > 50;
SELECT count() FROM t_carried_over WHERE v < 50;
SELECT countIf(explain ILIKE '%Statistics%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_carried_over WHERE v > 50);
SELECT countIf(explain ILIKE '%Statistics%') > 0 AND countIf(explain ILIKE '%Parts: 0/1%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_carried_over WHERE s > 1000);

CREATE TABLE t_rewritten (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         auto_statistics_types = '', enable_block_number_column = 1, enable_block_offset_column = 0;

INSERT INTO t_rewritten SELECT number FROM numbers(4);
ALTER TABLE t_rewritten ADD COLUMN v UInt32 DEFAULT 7 STATISTICS(basic);
ALTER TABLE t_rewritten ADD PROJECTION p (SELECT id, v ORDER BY v);
ALTER TABLE t_rewritten MATERIALIZE PROJECTION p;
ALTER TABLE t_rewritten MODIFY COLUMN v UInt32 DEFAULT 99;

SELECT 'a mutation that rewrites every column';
SELECT arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rewritten' AND active;
-- the projection froze `v` before the DEFAULT changed, so read past it to isolate the statistics
SELECT count() FROM t_rewritten WHERE v > 50 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_rewritten WHERE v < 50 SETTINGS optimize_use_projections = 0;
SELECT countIf(explain ILIKE '%Statistics%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_rewritten WHERE v > 50 SETTINGS optimize_use_projections = 0);

CREATE TABLE t_stored (id UInt64, w UInt32 STATISTICS(basic)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         auto_statistics_types = '', enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_stored SELECT number, 10 FROM numbers(4);
ALTER TABLE t_stored MATERIALIZE STATISTICS w;

SELECT 'a column the part stores keeps its statistics';
SELECT arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stored' AND active;
SELECT max(w), min(w) FROM t_stored;
SELECT count() FROM t_stored WHERE w > 1000;
SELECT count() FROM t_stored WHERE w = 10;
SELECT countIf(explain ILIKE '%Statistics%') > 0 AND countIf(explain ILIKE '%Parts: 0/1%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stored WHERE w > 1000);
SELECT countIf(explain ILIKE '%statistics_min_max%') > 0
FROM (EXPLAIN indexes = 1, projections = 1 SELECT max(w) FROM t_stored);

DROP TABLE t_carried_over;
DROP TABLE t_rewritten;
DROP TABLE t_stored;
