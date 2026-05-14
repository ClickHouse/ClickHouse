-- Tags: distributed
--
-- The `count_distinct_optimization` rewrite must not apply when the join-tree
-- root is a subquery (a `QueryNode`/`UnionNode` wrapper). Previously this
-- code path was missed by the local-only guard: only direct `TableNode` and
-- `TableFunctionNode` roots were checked, so a subquery wrapping a remote
-- source slipped through and was still rewritten. The visitor now fails close
-- for any non-table join-tree root.

SET enable_analyzer = 1; -- `EXPLAIN QUERY TREE` requires the analyzer

DROP TABLE IF EXISTS data_04236;

-- Use a `String` column so the local-table case actually triggers the rewrite.
CREATE TABLE data_04236 (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO data_04236 VALUES ('a'), ('a'), ('b'), ('b'), ('c');

SET count_distinct_optimization = 1;
SET prefer_localhost_replica = 0;

-- Subquery wrapping a `remote(...)` table function: the rewrite is NOT applied.
-- The aggregate stays as `uniqExact` and no extra `GROUP BY` subquery is added.
SELECT 'subquery over remote()';
SELECT sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact,
       sum(countSubstrings(explain, 'GROUP BY')) AS no_extra_group_by
FROM (EXPLAIN QUERY TREE run_passes = 1
      SELECT count(DISTINCT s)
      FROM (SELECT s FROM remote('127.0.0.{1,2}', currentDatabase(), data_04236)));

-- Subquery over a local table is also skipped under the fail-close guard.
-- The aggregate stays as `uniqExact`.
SELECT 'subquery over local table';
SELECT sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact
FROM (EXPLAIN QUERY TREE run_passes = 1
      SELECT count(DISTINCT s)
      FROM (SELECT s FROM data_04236));

-- Sanity check: results are still correct in both cases.
SELECT 'results';
SELECT count(DISTINCT s) FROM (SELECT s FROM data_04236);
SELECT count(DISTINCT s) FROM (SELECT s FROM remote('127.0.0.{1,2}', currentDatabase(), data_04236));

DROP TABLE data_04236;
