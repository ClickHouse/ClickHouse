-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `make_distributed_plan` serializes every plan fragment (`serializeQueryPlan` in
-- `DistributedPlanExecutor.cpp`), so a logical join really goes through
-- `JoinStepLogical::serializeSettings` here. The join stays logical in the fragment and is
-- physicalized on the worker, so an invalid `max_streams_per_hierarchical_merge` must not be
-- rejected up front for a hash-first algorithm list - only a fragment that actually builds a full
-- sort may reject it.

DROP TABLE IF EXISTS t_hier_merge_join_left;
DROP TABLE IF EXISTS t_hier_merge_join_right;

CREATE TABLE t_hier_merge_join_left (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_hier_merge_join_right (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_hier_merge_join_left SELECT number FROM numbers(10);
INSERT INTO t_hier_merge_join_right SELECT number FROM numbers(10);

SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET serialize_query_plan = 1;
SET max_streams_per_hierarchical_merge = 1;

SELECT '-- hash-first algorithm lists are carried through the serialized logical join --';

SELECT count()
FROM t_hier_merge_join_left AS l INNER JOIN t_hier_merge_join_right AS r ON l.id = r.id
SETTINGS join_algorithm = 'hash';

SELECT count()
FROM t_hier_merge_join_left AS l INNER JOIN t_hier_merge_join_right AS r ON l.id = r.id
SETTINGS join_algorithm = 'hash,full_sorting_merge';

SELECT count()
FROM t_hier_merge_join_left AS l INNER JOIN t_hier_merge_join_right AS r ON l.id = r.id
SETTINGS join_algorithm = 'hash,ie_join';

SELECT '-- a fragment that really builds a full sort rejects the invalid value --';

SELECT count()
FROM t_hier_merge_join_left AS l INNER JOIN t_hier_merge_join_right AS r ON l.id = r.id
SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError BAD_ARGUMENTS }

SELECT '-- valid values still execute the serialized full sort --';

SELECT count()
FROM t_hier_merge_join_left AS l INNER JOIN t_hier_merge_join_right AS r ON l.id = r.id
SETTINGS join_algorithm = 'full_sorting_merge', max_streams_per_hierarchical_merge = 0;

SELECT count()
FROM t_hier_merge_join_left AS l INNER JOIN t_hier_merge_join_right AS r ON l.id = r.id
SETTINGS join_algorithm = 'full_sorting_merge', max_streams_per_hierarchical_merge = 16;

DROP TABLE t_hier_merge_join_left;
DROP TABLE t_hier_merge_join_right;
