-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/112032
-- `transform_null_in` renames the `in` family during function resolution. A pass that creates an
-- `in`-family node afterwards emits the un-renamed name, a remote shard renames it while re-analyzing
-- the shipped AST, and the aggregate function name the initiator expects in the remote block is then
-- absent from it.

SET enable_analyzer = 1;
SET transform_null_in = 1;
SET optimize_rewrite_has_to_in = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;
SET rewrite_in_to_join = 0;
SET prefer_localhost_replica = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_112032;
CREATE TABLE t_112032 (id UInt64, nid Nullable(UInt64), tu Tuple(Nullable(Int32), Int32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_112032 SELECT number, if(number % 10 = 0, NULL, number), (if(number % 10 = 0, NULL, number), number) FROM numbers(100);

-- The reported query, and the same divergence for every other pass that creates an `in`-family node.
-- Mode 2 throws instead of falling back to a local read, whose answer is the same `6`.
SELECT 'has, parallel replicas', sumIf(id, has([1, 2, 3], id)) FROM t_112032
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_local_plan = 0,
         parallel_replicas_for_non_replicated_merge_tree = 1,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'notHas', countIf(notHas([1, 2, 3], id)) FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_112032);
SELECT 'equals chain', sumIf(id, id = 1 OR id = 2 OR id = 3) FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_112032);
SELECT 'notEquals chain', sumIf(id, id != 1 AND id != 2 AND id != 3) FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_112032);

-- Each rewrite must still fire, under the name resolution produces. Both settings are pinned so that
-- a guard which simply stopped rewriting would fail here.
SELECT 'has rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sumIf(id, has([1, 2, 3], id)) FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'has rewritten, setting off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sumIf(id, has([1, 2, 3], id)) FROM t_112032) WHERE explain ILIKE '%function_name: in%' SETTINGS transform_null_in = 0;
SELECT 'notHas rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT countIf(notHas([1, 2, 3], id)) FROM t_112032) WHERE explain ILIKE '%function_name: notNullIn%';
SELECT 'equals chain rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sumIf(id, id = 1 OR id = 2 OR id = 3) FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'notEquals chain rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sumIf(id, id != 1 AND id != 2 AND id != 3) FROM t_112032) WHERE explain ILIKE '%function_name: notNullIn%';

DROP DICTIONARY IF EXISTS d_112032;
CREATE DICTIONARY d_112032 (k UInt64, a String) PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'SELECT arrayJoin([1, 2, 3]) AS k, \'x\' AS a'))
LAYOUT(flat()) LIFETIME(0);

SELECT 'dictionary rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_112032 WHERE dictGet('d_112032', 'a', id) = 'x') WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'dictionary rewritten, setting off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_112032 WHERE dictGet('d_112032', 'a', id) = 'x') WHERE explain ILIKE '%function_name: in%' SETTINGS transform_null_in = 0;

-- `LIKE` is not an equality, so the constant-fold rewrite above does not apply to it and the
-- predicate reaches the pass's other producer, `key IN (SELECT key FROM dictionary(...))`.
SELECT 'dictionary subquery rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_112032 WHERE dictGet('d_112032', 'a', id) LIKE 'x%') WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'dictionary subquery rewritten, setting off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t_112032 WHERE dictGet('d_112032', 'a', id) LIKE 'x%') WHERE explain ILIKE '%function_name: in%' SETTINGS transform_null_in = 0;

-- `nullIn` compares a NULL left argument where `in` propagates it, so an expression that can itself
-- be NULL keeps its comparison chain. The values must stay those of the un-rewritten chain.
SELECT 'nullable declined', count(nid = 1 OR nid = 2 OR nid = 3), sum(nid = 1 OR nid = 2 OR nid = 3) FROM t_112032;
SELECT 'nullable declined, chain off', count(nid = 1 OR nid = 2 OR nid = 3), sum(nid = 1 OR nid = 2 OR nid = 3) FROM t_112032 SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'nullable not rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(nid = 1 OR nid = 2 OR nid = 3) FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%' OR explain ILIKE '%function_name: in%';
SELECT 'nullable rewritten, setting off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(nid = 1 OR nid = 2 OR nid = 3) FROM t_112032) WHERE explain ILIKE '%function_name: in%' SETTINGS transform_null_in = 0;

-- A simple-key dictionary permits a `Nullable` key expression, so both dictionary rewrites reach a
-- NULL-capable argument as well and decline it. `count` is what distinguishes a kept NULL from a `0`.
SELECT 'nullable dict declined', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032;
SELECT 'nullable dict declined, setting off', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032 SETTINGS transform_null_in = 0;
SELECT 'nullable dict not rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%' OR explain ILIKE '%function_name: in%';
SELECT 'nullable dict rewritten, setting off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032) WHERE explain ILIKE '%function_name: in%' SETTINGS transform_null_in = 0;

-- Only a NULL the argument holds itself is compared differently by the two names, so an expression
-- whose NULLs are nested stays rewritten, and the two names agree on its value.
SELECT 'nested nullable rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)) FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'nested nullable', count(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)), sum(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)) FROM t_112032;
SELECT 'nested nullable, setting off', count(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)), sum(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)) FROM t_112032 SETTINGS transform_null_in = 0;

DROP DICTIONARY d_112032;
DROP TABLE t_112032;
