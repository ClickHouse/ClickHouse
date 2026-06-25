-- Regression test for a logical error in correlated-subquery decorrelation with a set operation.
-- A set operation (INTERSECT / UNION ALL / EXCEPT) whose branches reference correlated subqueries
-- shares the input subplan through an in-memory producer/consumer buffer (SaveSubqueryResultToBuffer
-- fills it, ReadFromCommonBuffer reads it). The consumer may only run once every producer stream
-- finished. The decorrelation result join was built with correlated_subqueries_default_join_kind,
-- and with 'left' the buffer producer ended up on the join's probe side while a consumer was nested
-- on the build side. FillRightFirst then ran the consumer before the producer finished and the
-- server aborted with
--   Logical error: Trying to extract chunk from ChunkBuffer before all inputs are finished
-- The fix keeps the buffered (referenced) input on the build side regardless of the requested join
-- kind, so the producer is always evaluated first. The join kind is an internal detail and does not
-- change the result.
--
-- The buffer is only created when correlated_subqueries_use_in_memory_buffer = 1: with the buffer
-- disabled the optimizer's materialization pass replaces the shared subplan reference with an inline
-- copy (no ChunkBuffer), so that case never hits the ordering invariant. Both buffer settings are
-- covered below: = 1 is the case the fix protects, = 0 must keep returning the same rows.
--
-- Bug: https://github.com/ClickHouse/ClickHouse/issues/108521 (STID 2651-2cfd)

-- Correlated subqueries are only supported by the new analyzer.
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
-- Parallel replicas change the decorrelation execution path and row distribution; pin it off so the
-- per-part _part_offset row counts are deterministic (same as the sibling decorrelation test 03734).
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_chunk_buffer_set_op;

CREATE TABLE t_chunk_buffer_set_op (i Int32) ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1;
-- Two separate inserts so the table has two parts and _part_offset is exercised per part.
INSERT INTO t_chunk_buffer_set_op SELECT number FROM numbers(5);
INSERT INTO t_chunk_buffer_set_op SELECT number FROM numbers(5);

-- ------------------------------------------------------------------------------------------------
-- correlated_subqueries_use_in_memory_buffer = 1 (default): the common subplan is buffered directly.
-- ------------------------------------------------------------------------------------------------

-- INTERSECT with a correlated subquery in each branch, default_join_kind = 'left'. Both branches keep
-- the rows where 2 * i = 8, i.e. i = 4 (once per part). Before the fix this aborted the server.
SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'left'
INTERSECT
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

-- UNION ALL variant: i = 4 once per part on each branch, four rows total.
SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'left'
UNION ALL
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

-- The result must not depend on the internal decorrelation join kind: 'right' returns the same rows.
SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'right'
INTERSECT
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

-- ------------------------------------------------------------------------------------------------
-- correlated_subqueries_use_in_memory_buffer = 0: the materialization pass inlines the shared subplan
-- reference so no ChunkBuffer is created and the query is naturally safe. This variant is called out
-- in #108521; the rows must stay the same as the buffered case above.
-- ------------------------------------------------------------------------------------------------
SET correlated_subqueries_use_in_memory_buffer = 0;

SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'left'
INTERSECT
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'left'
UNION ALL
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'right'
INTERSECT
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

DROP TABLE t_chunk_buffer_set_op;
