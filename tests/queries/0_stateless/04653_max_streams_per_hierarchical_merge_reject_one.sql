-- Tags: no-darwin
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.

-- `max_streams_per_hierarchical_merge = 1` must be rejected rather than silently coerced to 2.
-- A merge node with a single input does not reduce the number of streams, so no merge tree can be
-- built from it. Accepting the value and quietly using 2 would make `system.settings` disagree with
-- the pipeline that is actually built.

SELECT '-- assignment itself is accepted, the value is only rejected when a full-sort pipeline is built --';

SET max_streams_per_hierarchical_merge = 1;
SELECT getSetting('max_streams_per_hierarchical_merge');
-- No full sort here, so nothing validates the setting.
SELECT count() FROM numbers(10);

SELECT '-- rejected on the full sort path --';
SELECT number FROM numbers(10) ORDER BY number; -- { serverError BAD_ARGUMENTS }

SELECT '-- 0 and values >= 2 are accepted --';
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number SETTINGS max_streams_per_hierarchical_merge = 0);
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number SETTINGS max_streams_per_hierarchical_merge = 2);
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number SETTINGS max_streams_per_hierarchical_merge = 16);

SELECT '-- rejected on the serialized query plan path --';
-- `serialize_query_plan = 1` alone does not serialize a local query: only distributed plan fragments
-- are serialized. `make_distributed_plan` serializes every fragment (`serializeQueryPlan` in
-- `DistributedPlanExecutor.cpp`), so the full sort below is really rebuilt from the serialized
-- plan settings on the worker. The fan-in itself remains local; serialization version 20 carries
-- the original value only for validation, and the rebuilt full sort rejects it when its pipeline
-- is built.
DROP TABLE IF EXISTS t_hier_merge_reject_one;
CREATE TABLE t_hier_merge_reject_one (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_hier_merge_reject_one SELECT number FROM numbers(10);

SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET serialize_query_plan = 1;

SELECT id FROM t_hier_merge_reject_one ORDER BY id
SETTINGS max_streams_per_hierarchical_merge = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- the same for a full sorting merge join --';
SELECT count() FROM t_hier_merge_reject_one AS l JOIN t_hier_merge_reject_one AS r ON l.id = r.id
SETTINGS join_algorithm = 'full_sorting_merge', max_streams_per_hierarchical_merge = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 0 and values >= 2 are accepted on the serialized query plan path --';
SELECT count() FROM (SELECT id FROM t_hier_merge_reject_one ORDER BY id) SETTINGS max_streams_per_hierarchical_merge = 0;
SELECT count() FROM (SELECT id FROM t_hier_merge_reject_one ORDER BY id) SETTINGS max_streams_per_hierarchical_merge = 16;

DROP TABLE t_hier_merge_reject_one;
