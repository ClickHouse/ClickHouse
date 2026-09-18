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
-- NULL-capable argument as well. There the null-aware name is emitted with the NULL it compares
-- restored explicitly, so the value stays that of the comparison. `count` is what distinguishes a
-- kept NULL from a `0`.
SELECT 'nullable dict null restored', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032;
SELECT 'nullable dict declined, setting off', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032 SETTINGS transform_null_in = 0;
SELECT 'nullable dict rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%' OR explain ILIKE '%function_name: in%';
SELECT 'nullable dict rewritten, setting off', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', nid) = 'x') FROM t_112032) WHERE explain ILIKE '%function_name: in%' SETTINGS transform_null_in = 0;

-- Only a NULL the argument holds itself is compared differently by the two names, so an expression
-- whose NULLs are nested stays rewritten, and the two names agree on its value.
SELECT 'nested nullable rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)) FROM t_112032) WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'nested nullable', count(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)), sum(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)) FROM t_112032;
SELECT 'nested nullable, setting off', count(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)), sum(tu = (1, 1) OR tu = (2, 2) OR tu = (3, 3)) FROM t_112032 SETTINGS transform_null_in = 0;

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/120650
-- The restored NULL sits outside the set membership, and index analysis does not look through it, so
-- the same membership node is handed to index analysis as an `indexHint` conjunct of the filter. Only
-- a conjunct reachable from the filter root through `and` alone is implied by the filter being true.
SET allow_suspicious_low_cardinality_types = 1;
SET use_skip_indexes = 1;
SET use_index_for_in_with_subqueries = 1;

DROP TABLE IF EXISTS t2_120650;
CREATE TABLE t2_120650 (nid Nullable(UInt64), lc LowCardinality(Nullable(UInt64)), bfc Nullable(UInt64),
                        INDEX bf_idx bfc TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY nid
SETTINGS allow_nullable_key = 1, index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO t2_120650 SELECT if(number % 10 = 0, NULL, number), if(number % 10 = 0, NULL, number),
                             if(number % 10 = 0, NULL, number) FROM numbers(1000);

DROP TABLE IF EXISTS t3_120650;
CREATE TABLE t3_120650 (lc LowCardinality(Nullable(UInt64))) ENGINE = MergeTree ORDER BY lc
SETTINGS allow_nullable_key = 1, index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO t3_120650 SELECT if(number % 10 = 0, NULL, number) FROM numbers(1000);

-- `force_primary_key` throws when the key condition is unusable, so it asserts the pruning without
-- putting a granule count in the reference. Each row is followed by the same query with the rewrite
-- off, which must throw: that is what proves the assertion above it can fail.
SELECT 'const arm, key pruned', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' SETTINGS force_primary_key = 1;
SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
SELECT 'subquery arm, key pruned', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%' SETTINGS force_primary_key = 1;
SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%' SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
SELECT 'lowcardinality key pruned', count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x' SETTINGS force_primary_key = 1;
SELECT count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x' SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
SELECT 'and spine, key pruned', count() FROM t2_120650 WHERE (dictGet('d_112032', 'a', nid) = 'x' AND bfc IS NOT NULL) AND lc IS NOT NULL SETTINGS force_primary_key = 1;
SELECT count() FROM t2_120650 WHERE (dictGet('d_112032', 'a', nid) = 'x' AND bfc IS NOT NULL) AND lc IS NOT NULL SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
SELECT 'prewhere, key pruned', count() FROM t2_120650 PREWHERE dictGet('d_112032', 'a', nid) = 'x' SETTINGS force_primary_key = 1;
SELECT count() FROM t2_120650 PREWHERE dictGet('d_112032', 'a', nid) = 'x' SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
SELECT 'and with a second set, key pruned', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' AND bfc IN (SELECT toUInt64(2)) SETTINGS force_primary_key = 1;
SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' AND bfc IN (SELECT toUInt64(2)) SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
WITH dictGet('d_112032', 'a', nid) = 'x' AS p SELECT 'shared node, key pruned', count(p) FROM t2_120650 WHERE p SETTINGS force_primary_key = 1;
WITH dictGet('d_112032', 'a', nid) = 'x' AS p SELECT count(p) FROM t2_120650 WHERE p SETTINGS force_primary_key = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }
SELECT 'skip index used', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', bfc) = 'x' SETTINGS force_data_skipping_indices = 'bf_idx';
SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', bfc) = 'x' SETTINGS force_data_skipping_indices = 'bf_idx', optimize_inverse_dictionary_lookup = 0; -- { serverError INDEX_NOT_USED }

-- `force_primary_key` and `force_data_skipping_indices` throw on an unusable condition; neither checks
-- that less data is read. The granules an index selects, compared with the granules it saw as a
-- boolean, assert that too without putting a part layout in the reference. `toUInt64OrZero` makes an
-- unexpected line shape print `0` rather than throw, so the row still fails loudly.
SELECT 'granules pruned, const arm', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x') WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, const arm, rewrite off', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0) WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, subquery arm', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%') WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, subquery arm, rewrite off', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%' SETTINGS optimize_inverse_dictionary_lookup = 0) WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, lowcardinality PK', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x') WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, lowcardinality PK, rewrite off', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0) WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, skip index', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', bfc) = 'x') WHERE explain ILIKE '%Granules:%';
SELECT 'granules pruned, skip index, rewrite off', countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0
  FROM (EXPLAIN indexes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', bfc) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0) WHERE explain ILIKE '%Granules:%';

-- The filter can be true where a conjunct under `not` or `or` is false, so those positions must not
-- reach index analysis. A row here that stops throwing is a wrong-results bug, not a test nit.
SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' OR nid = 501 SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x') AND bfc IS NOT NULL SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t2_120650 WHERE (dictGet('d_112032', 'a', nid) = 'x' OR nid = 501) AND bfc IS NOT NULL SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- Every value must be the one the un-rewritten query returns, both with the renaming off and with the
-- rewrite off. The `not` and `or` columns are the ones a hint in an unsound position would change.
SELECT 'where values',
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x'),
  (SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x')),
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' OR nid = 501),
  (SELECT count() FROM t2_120650 PREWHERE dictGet('d_112032', 'a', nid) = 'x'),
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%'),
  (SELECT count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x');
SELECT 'where values, setting off',
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x'),
  (SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x')),
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' OR nid = 501),
  (SELECT count() FROM t2_120650 PREWHERE dictGet('d_112032', 'a', nid) = 'x'),
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%'),
  (SELECT count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x') SETTINGS transform_null_in = 0;
SELECT 'where values, rewrite off',
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x'),
  (SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x')),
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' OR nid = 501),
  (SELECT count() FROM t2_120650 PREWHERE dictGet('d_112032', 'a', nid) = 'x'),
  (SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%'),
  (SELECT count() FROM t3_120650 WHERE dictGet('d_112032', 'a', lc) = 'x') SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'not inside and value', count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x') AND bfc IS NOT NULL;
SELECT 'not inside and value, setting off', count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x') AND bfc IS NOT NULL SETTINGS transform_null_in = 0;
SELECT 'or inside and value', count() FROM t2_120650 WHERE (dictGet('d_112032', 'a', nid) = 'x' OR nid = 501) AND bfc IS NOT NULL;
SELECT 'or inside and value, setting off', count() FROM t2_120650 WHERE (dictGet('d_112032', 'a', nid) = 'x' OR nid = 501) AND bfc IS NOT NULL SETTINGS transform_null_in = 0;
SELECT 'and with a second set value', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' AND bfc IN (SELECT toUInt64(2));
SELECT 'and with a second set value, setting off', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' AND bfc IN (SELECT toUInt64(2)) SETTINGS transform_null_in = 0;
SELECT 'and with a second set value, rewrite off', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' AND bfc IN (SELECT toUInt64(2)) SETTINGS optimize_inverse_dictionary_lookup = 0;

-- A `count` over the predicate itself is what a missing NULL restoration changes: it counts 1000
-- instead of 900, because the null-aware name answers `0` where the comparison answers NULL. A
-- `LowCardinality(Nullable)` key reaches index analysis through a `_CAST` the other keys do not, so
-- both arms of that carrier are counted here too; in a `WHERE` a dropped NULL and a `0` are both
-- filtered out and no row above would move.
SELECT 'projection values', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x'),
       count(dictGet('d_112032', 'a', nid) LIKE 'x%'), sum(dictGet('d_112032', 'a', nid) LIKE 'x%'),
       count(dictGet('d_112032', 'a', lc) = 'x'), sum(dictGet('d_112032', 'a', lc) = 'x'),
       count(dictGet('d_112032', 'a', lc) LIKE 'x%'), sum(dictGet('d_112032', 'a', lc) LIKE 'x%') FROM t2_120650;
SELECT 'projection values, setting off', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x'),
       count(dictGet('d_112032', 'a', nid) LIKE 'x%'), sum(dictGet('d_112032', 'a', nid) LIKE 'x%'),
       count(dictGet('d_112032', 'a', lc) = 'x'), sum(dictGet('d_112032', 'a', lc) = 'x'),
       count(dictGet('d_112032', 'a', lc) LIKE 'x%'), sum(dictGet('d_112032', 'a', lc) LIKE 'x%') FROM t2_120650 SETTINGS transform_null_in = 0;
SELECT 'projection values, rewrite off', count(dictGet('d_112032', 'a', nid) = 'x'), sum(dictGet('d_112032', 'a', nid) = 'x'),
       count(dictGet('d_112032', 'a', nid) LIKE 'x%'), sum(dictGet('d_112032', 'a', nid) LIKE 'x%'),
       count(dictGet('d_112032', 'a', lc) = 'x'), sum(dictGet('d_112032', 'a', lc) = 'x'),
       count(dictGet('d_112032', 'a', lc) LIKE 'x%'), sum(dictGet('d_112032', 'a', lc) LIKE 'x%') FROM t2_120650 SETTINGS optimize_inverse_dictionary_lookup = 0;
WITH dictGet('d_112032', 'a', nid) = 'x' AS p SELECT 'shared node value', count(p) FROM t2_120650 WHERE p;
WITH dictGet('d_112032', 'a', nid) = 'x' AS p SELECT 'shared node value, setting off', count(p) FROM t2_120650 WHERE p SETTINGS transform_null_in = 0;
WITH dictGet('d_112032', 'a', nid) = 'x' AS p SELECT 'shared node value, rewrite off', count(p) FROM t2_120650 WHERE p SETTINGS optimize_inverse_dictionary_lookup = 0;

-- No key maps to the constant, so the membership set is empty and the hint prunes every granule: the
-- one shape where an unsound implication would drop all rows rather than a few. The predicate is still
-- NULL for a NULL key, so the counts stay those of the un-rewritten query.
SELECT 'empty key set', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'zzz';
SELECT 'empty key set projection', count(dictGet('d_112032', 'a', nid) = 'zzz'), sum(dictGet('d_112032', 'a', nid) = 'zzz') FROM t2_120650;
SELECT 'empty key set projection, rewrite off', count(dictGet('d_112032', 'a', nid) = 'zzz'), sum(dictGet('d_112032', 'a', nid) = 'zzz') FROM t2_120650 SETTINGS optimize_inverse_dictionary_lookup = 0;

-- The hint is counted, not merely looked for: an alias reaches the pass once but the tree twice, and
-- only the filter's occurrence may be hinted. `dictGet` is gone, so no per-row lookup is left behind.
SELECT 'where form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x') WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'where form nullIn', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x') WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'where form if', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x') WHERE explain ILIKE '%function_name: if%';
SELECT 'where form dictGet', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x') WHERE explain ILIKE '%function_name: dictGet%';
SELECT 'prewhere form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 PREWHERE dictGet('d_112032', 'a', nid) = 'x') WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'subquery arm indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%') WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'subquery arm dictGet', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) LIKE 'x%') WHERE explain ILIKE '%function_name: dictGet%';
SELECT 'projection form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', nid) = 'x') FROM t2_120650) WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'projection form nullIn', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', nid) = 'x') FROM t2_120650) WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'not form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x')) WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'or form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x' OR nid = 501) WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'not inside and form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE NOT (dictGet('d_112032', 'a', nid) = 'x') AND bfc IS NOT NULL) WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'or inside and form indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE (dictGet('d_112032', 'a', nid) = 'x' OR nid = 501) AND bfc IS NOT NULL) WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'shared node indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 WITH dictGet('d_112032', 'a', nid) = 'x' AS p SELECT count(p) FROM t2_120650 WHERE p) WHERE explain ILIKE '%function_name: indexHint%';
SELECT 'not null key rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', assumeNotNull(nid)) = 'x') WHERE explain ILIKE '%function_name: nullIn%';
SELECT 'not null key indexHint', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM t2_120650 WHERE dictGet('d_112032', 'a', assumeNotNull(nid)) = 'x') WHERE explain ILIKE '%function_name: indexHint%';

-- The hint travels inside `WHERE`, so these also exercise the round trip of the whole rewritten
-- filter through a shard, which is the divergence the decline above was protecting against.
SELECT 'remote const arm', sumIf(1, dictGet('d_112032', 'a', nid) = 'x') FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t2_120650);
SELECT 'remote subquery arm', sumIf(1, dictGet('d_112032', 'a', nid) LIKE 'x%') FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t2_120650);
SELECT 'remote where const arm', count() FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t2_120650) WHERE dictGet('d_112032', 'a', nid) = 'x';
SELECT 'remote where subquery arm', count() FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t2_120650) WHERE dictGet('d_112032', 'a', nid) LIKE 'x%';
SELECT 'dict, parallel replicas', sumIf(1, dictGet('d_112032', 'a', nid) = 'x') FROM t2_120650
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_local_plan = 0,
         parallel_replicas_for_non_replicated_merge_tree = 1,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SELECT 'dict where, parallel replicas', count() FROM t2_120650 WHERE dictGet('d_112032', 'a', nid) = 'x'
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_local_plan = 0,
         parallel_replicas_for_non_replicated_merge_tree = 1,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The replacement consumes a real set in every position, exactly as the un-renamed rewrite does, so
-- a set size limit must reach the same outcome with the renaming on as with it off.
SELECT count(dictGet('d_112032', 'a', nid) LIKE 'x%') FROM t2_120650 SETTINGS max_rows_in_set = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count(dictGet('d_112032', 'a', nid) LIKE 'x%') FROM t2_120650 SETTINGS max_rows_in_set = 1, transform_null_in = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `dictGet` does not propagate the NULL of a key that carries it in a discriminator, and the `in`
-- family rejects a dynamic structure, so those keys keep the comparison they had.
DROP TABLE IF EXISTS tv_120650;
CREATE TABLE tv_120650 (vk Variant(UInt64, String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tv_120650 SELECT if(number % 10 = 0, NULL, number::Variant(UInt64, String)) FROM numbers(100);
SELECT 'variant key value', count(dictGet('d_112032', 'a', vk) = 'x'), sum(dictGet('d_112032', 'a', vk) = 'x') FROM tv_120650;
SELECT 'variant key value, rewrite off', count(dictGet('d_112032', 'a', vk) = 'x'), sum(dictGet('d_112032', 'a', vk) = 'x') FROM tv_120650 SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'variant key not rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', vk) = 'x') FROM tv_120650) WHERE explain ILIKE '%function_name: nullIn%' OR explain ILIKE '%function_name: indexHint%';

DROP TABLE IF EXISTS td_120650;
CREATE TABLE td_120650 (dk Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO td_120650 SELECT if(number % 10 = 0, NULL, number::Dynamic) FROM numbers(100);
SELECT 'dynamic key value', count(dictGet('d_112032', 'a', dk) = 'x'), sum(dictGet('d_112032', 'a', dk) = 'x') FROM td_120650;
SELECT 'dynamic key value, rewrite off', count(dictGet('d_112032', 'a', dk) = 'x'), sum(dictGet('d_112032', 'a', dk) = 'x') FROM td_120650 SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'dynamic key not rewritten', count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(dictGet('d_112032', 'a', dk) = 'x') FROM td_120650) WHERE explain ILIKE '%function_name: nullIn%' OR explain ILIKE '%function_name: indexHint%';

DROP TABLE td_120650;
DROP TABLE tv_120650;
DROP TABLE t3_120650;
DROP TABLE t2_120650;
DROP DICTIONARY d_112032;
DROP TABLE t_112032;
