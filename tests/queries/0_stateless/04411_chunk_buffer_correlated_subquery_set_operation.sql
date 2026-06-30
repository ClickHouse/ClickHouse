-- Regression test for issue #108521 (STID 2651-2cfd): a set operation (INTERSECT / UNION ALL / EXCEPT)
-- over correlated subqueries with correlated_subqueries_default_join_kind = 'left' aborted the server
-- with "Trying to extract chunk from ChunkBuffer before all inputs are finished".
--
-- correlated_subqueries_use_in_memory_buffer controls whether the shared subplan is buffered, but the
-- decision is made from the top-level query context, so the same set operation must return the same
-- rows for all three placements of the setting:
--   * default (buffer on): a buffer is created;
--   * session-level SET ... = 0: no buffer (the reference is materialized);
--   * per-branch SETTINGS ... = 0 (the #108521 shape): a buffer is still created.

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
