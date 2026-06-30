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
-- Whether a buffer is created is decided once for the whole plan from the top-level query context
-- (correlated_subqueries_use_in_memory_buffer there gates the materialization pass that would inline
-- the shared subplan reference). The decorrelation join layout must follow that same top-level
-- decision. A set-operation branch's own SETTINGS clause does not reach the materialization pass, so
-- correlated_subqueries_use_in_memory_buffer = 0 set per-branch still leaves the buffer in place; the
-- protection must fire there too. All three placements are covered below and must return the same rows:
--   * default (buffer on): the buffer is created and the fix forces the safe layout.
--   * session-level SET ... = 0: reaches the materialization pass, no buffer, plain LEFT layout is safe.
--   * per-branch SETTINGS ... = 0: does NOT reach the materialization pass, buffer is still created,
--     so the fix must force the safe layout here too (the case this regression specifically guards).
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
-- Session-level correlated_subqueries_use_in_memory_buffer = 0: reaches the top-level query context,
-- so the materialization pass inlines the shared subplan reference and no ChunkBuffer is created. The
-- plain LEFT layout is safe here; the rows must stay the same as the buffered case above.
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

SET correlated_subqueries_use_in_memory_buffer = 1;

-- ------------------------------------------------------------------------------------------------
-- Per-branch SETTINGS correlated_subqueries_use_in_memory_buffer = 0 (the exact shape from #108521):
-- the branch SETTINGS clause does not reach the top-level materialization pass, so with the default
-- session value the buffer is still created. The decorrelation layout must follow the actual buffer
-- decision and force the safe layout here, otherwise the server aborts with the ChunkBuffer error.
-- The rows must match the cases above.
-- ------------------------------------------------------------------------------------------------
SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'left',
           correlated_subqueries_use_in_memory_buffer = 0
INTERSECT
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

SELECT i FROM t_chunk_buffer_set_op WHERE 8 = ((SELECT _part_offset) + i)
  SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0,
           correlated_subqueries_default_join_kind = 'left',
           correlated_subqueries_use_in_memory_buffer = 0
UNION ALL
SELECT i FROM t_chunk_buffer_set_op WHERE 8 <=> (i + (SELECT _part_offset))
ORDER BY i;

DROP TABLE t_chunk_buffer_set_op;
