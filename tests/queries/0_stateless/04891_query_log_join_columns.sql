-- Tags: no-old-analyzer

-- `used_number_of_joins` counts the physical joins of the executed pipeline, `used_join_algorithms`,
-- `used_join_kinds` and `used_join_strictness` describe them, and `spilled_to_disk` lists the
-- operators that wrote temporary data, which is not limited to joins.
--
-- The algorithm is set explicitly wherever it is asserted, because the choice among the algorithms
-- allowed by `join_algorithm` is made at run time and depends on the number of threads.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT. Disable that here so the kinds are the ones the queries are written
-- with; the block that covers the swap turns it back on for its own queries.
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS tj;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;

CREATE TABLE t1 (a UInt64) ENGINE = Memory;
CREATE TABLE t2 (a UInt64) ENGINE = Memory;
CREATE TABLE t3 (a UInt64) ENGINE = Memory;
CREATE TABLE tj (a UInt64, b UInt64) ENGINE = Join(ANY, LEFT, a);
CREATE TABLE ta (a UInt64, t UInt64) ENGINE = Memory;
CREATE TABLE tb (a UInt64, t UInt64) ENGINE = Memory;
-- `EXPLAIN ESTIMATE` reports parts and marks, so it needs MergeTree.
CREATE TABLE m1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE m2 (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number FROM numbers(10);
INSERT INTO t3 SELECT number FROM numbers(10);
INSERT INTO tj SELECT number, number FROM numbers(10);
INSERT INTO ta SELECT number, number * 2 FROM numbers(10);
INSERT INTO tb SELECT number, number FROM numbers(10);
INSERT INTO m1 SELECT number FROM numbers(10);
INSERT INTO m2 SELECT number FROM numbers(10);

SELECT 'no join';
SELECT count() FROM t1 FORMAT Null SETTINGS log_comment = '04891_join_count_none';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_none%'
ORDER BY log_comment;

SELECT 'single inner join';
-- The strictness is not written, so it comes from `join_default_strictness`.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_inner', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_inner%'
ORDER BY log_comment;

SELECT 'join executed with its sides swapped';
-- `query_plan_join_swap_table` lets the optimizer execute the join the other way around, building the
-- hash table from the left table instead of the right one. That reverses the kind of the join it runs,
-- and the reported kind is the executed one, so a RIGHT JOIN of the query text is reported as LEFT.
--
-- The swap is decided by the join order optimizer, which `query_plan_optimize_join_order_limit = 0`
-- turns off entirely. With it off, `query_plan_join_swap_table` is never read and the join runs with
-- the sides of the query text, so the limit has to be pinned here for the swap to be forced at all.
SELECT count() FROM t1 RIGHT JOIN t2 ON t1.a = t2.a FORMAT Null
SETTINGS log_comment = '04891_join_count_swap_a_right_swapped', join_algorithm = 'hash', query_plan_join_swap_table = 1, query_plan_optimize_join_order_limit = 10;
SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a FORMAT Null
SETTINGS log_comment = '04891_join_count_swap_b_left_swapped', join_algorithm = 'hash', query_plan_join_swap_table = 1, query_plan_optimize_join_order_limit = 10;
-- The same RIGHT JOIN without the swap, which reports the kind of the query text.
SELECT count() FROM t1 RIGHT JOIN t2 ON t1.a = t2.a FORMAT Null
SETTINGS log_comment = '04891_join_count_swap_c_right_not_swapped', join_algorithm = 'hash', query_plan_join_swap_table = 0;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_swap%'
ORDER BY log_comment;

SELECT 'three tables, two joins';
-- Both joins have the same kind and strictness, and both are reported, so the arrays hold one element
-- per join and their size matches the count.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a FORMAT Null SETTINGS log_comment = '04891_join_count_two', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_two%'
ORDER BY log_comment;

SELECT 'cross join';
-- Strictness is meaningless for a CROSS join, and the reported one is whatever the join carries,
-- which is `join_default_strictness`. It has no `ON` clause, so its condition is a constant and it
-- is executed by `ConstantJoin` rather than by the requested `hash` algorithm. That is why every
-- join reports an algorithm of its own: otherwise a plain CROSS JOIN would show one executed join
-- and an empty list of algorithms.
SELECT count() FROM t1, t2 FORMAT Null SETTINGS log_comment = '04891_join_count_cross', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_cross%'
ORDER BY log_comment;

SELECT 'join engine table';
-- It is joined by `FilledJoinStep` and not by `JoinStep`.
SELECT count() FROM t1 ANY LEFT JOIN tj USING (a) FORMAT Null SETTINGS log_comment = '04891_join_count_filled';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_filled%'
ORDER BY log_comment;

SELECT 'asof join';
-- ASOF is a strictness and not a kind, so it is reported in `used_join_strictness`.
SELECT count() FROM ta ASOF LEFT JOIN tb USING (a, t) FORMAT Null SETTINGS log_comment = '04891_join_count_asof', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_asof%'
ORDER BY log_comment;

SELECT 'paste join';
-- PASTE is not one of the `join_algorithm` values, but it still reports itself, so that a query
-- whose only join is a paste join does not show an empty list of algorithms. Its strictness comes
-- from `join_default_strictness` as well, and it is as meaningless as for a CROSS join.
SELECT count() FROM (SELECT number AS a FROM numbers(10)) p1 PASTE JOIN (SELECT number AS a FROM numbers(10)) p2 FORMAT Null SETTINGS log_comment = '04891_join_count_paste';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_paste%'
ORDER BY log_comment;

SELECT 'every kind and strictness';
-- One query per reachable pair of `JoinKind` and `JoinStrictness` (see `Core/Joins.h`), so that the two
-- columns are covered systematically. The pairs above are covered again here on purpose: the grid is only
-- useful if it is complete on its own. The `log_comment` is selected along with the columns because 22
-- unnamed rows cannot be checked by eye.
--
-- Two values of the enums never reach the columns:
--  * `COMMA` - a comma join is rewritten before the pipeline is built, into `CROSS` on its own and into
--    `INNER` when the WHERE clause has a condition on both of its tables. Both cases are below.
--  * `UNSPECIFIED` - every kind gets a strictness. The ones for which it is meaningless (CROSS, COMMA,
--    PASTE) take `join_default_strictness`, which cannot be left empty for them either: with
--    `join_default_strictness = ''` a CROSS JOIN still reports ALL and a PASTE JOIN is rejected.
--
-- ALL, the strictness a join without one takes from `join_default_strictness`.
SELECT count() FROM t1 ALL INNER JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_01_all_inner';
SELECT count() FROM t1 ALL LEFT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_02_all_left';
SELECT count() FROM t1 ALL RIGHT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_03_all_right';
SELECT count() FROM t1 ALL FULL JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_04_all_full';
-- ANY.
SELECT count() FROM t1 ANY INNER JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_05_any_inner';
SELECT count() FROM t1 ANY LEFT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_06_any_left';
SELECT count() FROM t1 ANY RIGHT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_07_any_right';
-- RIGHT_ANY is the old meaning of ANY, selected by `any_join_distinct_right_table_keys`. Under that
-- setting an ANY INNER JOIN becomes a SEMI LEFT JOIN instead.
SELECT count() FROM t1 ANY LEFT JOIN t2 ON t1.a = t2.a
FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_08_right_any_left', any_join_distinct_right_table_keys = 1;
SELECT count() FROM t1 ANY RIGHT JOIN t2 ON t1.a = t2.a
FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_09_right_any_right', any_join_distinct_right_table_keys = 1;
SELECT count() FROM t1 ANY INNER JOIN t2 ON t1.a = t2.a
FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_10_any_inner_rewritten_to_semi_left', any_join_distinct_right_table_keys = 1;
-- SEMI and ANTI, which exist for LEFT and RIGHT only. Without a side they are LEFT.
SELECT count() FROM t1 SEMI LEFT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_11_semi_left';
SELECT count() FROM t1 SEMI RIGHT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_12_semi_right';
SELECT count() FROM t1 SEMI JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_13_semi_without_side';
SELECT count() FROM t1 ANTI LEFT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_14_anti_left';
SELECT count() FROM t1 ANTI RIGHT JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_15_anti_right';
SELECT count() FROM t1 ANTI JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_16_anti_without_side';
-- ASOF, which exists for INNER and LEFT only, on the tables that have a column to compare.
SELECT count() FROM ta ASOF JOIN tb ON ta.a = tb.a AND ta.t >= tb.t FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_17_asof_inner';
SELECT count() FROM ta ASOF LEFT JOIN tb ON ta.a = tb.a AND ta.t >= tb.t FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_18_asof_left';
-- The kinds for which strictness is meaningless.
SELECT count() FROM t1 CROSS JOIN t2 FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_19_cross';
SELECT count() FROM t1, t2 FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_20_comma_reported_as_cross';
SELECT count() FROM t1, t2 WHERE t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_21_comma_reported_as_inner';
SELECT count() FROM (SELECT number AS a FROM numbers(10)) p1 PASTE JOIN (SELECT number AS a FROM numbers(10)) p2
FORMAT Null SETTINGS log_comment = '04891_join_count_matrix_22_paste';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_matrix\_%'
ORDER BY log_comment;

-- The pairs that cannot be expressed at all. Nothing is logged for them, the query does not run.
SELECT count() FROM t1 ANY FULL JOIN t2 ON t1.a = t2.a; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM ta ASOF RIGHT JOIN tb ON ta.a = tb.a AND ta.t >= tb.t; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM ta ASOF FULL JOIN tb ON ta.a = tb.a AND ta.t >= tb.t; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 SEMI FULL JOIN t2 ON t1.a = t2.a; -- { clientError SYNTAX_ERROR }
SELECT count() FROM t1 ANTI FULL JOIN t2 ON t1.a = t2.a; -- { clientError SYNTAX_ERROR }

SELECT 'full sorting merge';
-- It keeps everything in memory here.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_fsm', join_algorithm = 'full_sorting_merge';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_fsm%'
ORDER BY log_comment;

SELECT 'parallel full sorting merge';
-- `parallel_full_sorting_merge` builds the same `FullSortingMergeJoin` as `full_sorting_merge`, and the
-- parallel variant only materializes when the join is really sharded, which needs more than one shard
-- (`max_threads`). The reported algorithm follows the sharding, not the setting.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04891_join_count_pfsm_sharded', join_algorithm = 'parallel_full_sorting_merge', max_threads = 4;
-- An ASOF join is never sharded by the hash of the key list: the trailing key is the inequality key, so
-- rows with equal equality keys could land in different shards and the closest match could be missed. It
-- runs as a single merge join and reports `full_sorting_merge`, even though the parallel variant was asked
-- for.
SELECT count() FROM ta ASOF LEFT JOIN tb USING (a, t) FORMAT Null SETTINGS log_comment = '04891_join_count_pfsm_serial', join_algorithm = 'parallel_full_sorting_merge', max_threads = 4;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_pfsm%'
ORDER BY log_comment;

SELECT 'ie join';
-- `ie_join` has no `IJoin`: the whole algorithm is `IEJoinStep`, which reports itself. It is only
-- picked for two inequality conditions.
SELECT count() FROM (SELECT number AS t FROM numbers(10)) i1 JOIN (SELECT number AS t_lo, number + 5 AS t_hi FROM numbers(10)) i2 ON i1.t >= i2.t_lo AND i1.t <= i2.t_hi
FORMAT Null
SETTINGS log_comment = '04891_join_count_ie_inner', join_algorithm = 'ie_join,hash';
-- A right-side ANTI join is executed as its left-side mirror, with the input pipelines swapped, and the
-- reported kind is the executed one, so it is LEFT rather than the RIGHT of the query text.
SELECT count() FROM (SELECT number AS t FROM numbers(10)) i1 RIGHT ANTI JOIN (SELECT number AS t_lo, number + 5 AS t_hi FROM numbers(10)) i2 ON i1.t >= i2.t_lo AND i1.t <= i2.t_hi
FORMAT Null
SETTINGS log_comment = '04891_join_count_ie_right_anti', join_algorithm = 'ie_join,hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_ie%'
ORDER BY log_comment;

SELECT 'explain reports the joins of the pipeline it builds';
-- These columns describe the pipeline that was built for the query, like the other `used_` columns of
-- `system.query_log` describe what the query instantiated while it was analyzed. An `EXPLAIN` that
-- assembles a pipeline therefore reports the joins of the explained query, whether it goes on to run
-- the pipeline or throws it away, and `used_functions` of the same `EXPLAIN` lists the functions of
-- the explained query in the same way.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a)
FORMAT Null
SETTINGS log_comment = '04891_join_count_explain_pipeline', join_algorithm = 'hash';
SELECT count() > 0 FROM (EXPLAIN ESTIMATE SELECT count() FROM m1 JOIN m2 ON m1.a = m2.a)
FORMAT Null
SETTINGS log_comment = '04891_join_count_explain_estimate', join_algorithm = 'hash';
SELECT count() > 0 FROM (EXPLAIN ANALYZE SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a)
FORMAT Null
SETTINGS log_comment = '04891_join_count_explain_analyze', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_explain%'
ORDER BY log_comment;

SELECT 'grace hash spilling';
-- With many buckets it writes the buckets it is not currently joining to disk.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) g1 JOIN (SELECT number AS a FROM numbers(10000)) g2 ON g1.a = g2.a
FORMAT Null
SETTINGS log_comment = '04891_join_count_grace_spill', join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32, grace_hash_join_max_buckets = 32;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_grace\_spill%'
ORDER BY log_comment;

SELECT 'partial merge spilling';
-- Over its memory limit it sorts the right table into temporary files.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) m1 JOIN (SELECT number AS a FROM numbers(10000)) m2 ON m1.a = m2.a
FORMAT Null
SETTINGS log_comment = '04891_join_count_merge_spill', join_algorithm = 'partial_merge', default_max_bytes_in_join = 0, max_bytes_in_join = 1024;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_merge\_spill%'
ORDER BY log_comment;

SELECT 'hash switching to grace hash';
-- A hash join that exceeds `max_bytes_before_external_join` is replaced by `grace_hash` while the
-- query is already running, so both algorithms are reported for the one join. The threshold has to
-- be well above zero: `grace_hash` keeps doubling the number of buckets to get each of them under
-- the threshold, and a threshold of a few bytes is unreachable, so it hits
-- `grace_hash_join_max_buckets` and the query fails instead.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) s1 JOIN (SELECT number AS a FROM numbers(10000)) s2 ON s1.a = s2.a
FORMAT Null
SETTINGS log_comment = '04891_join_count_switch', join_algorithm = 'hash', max_bytes_before_external_join = 65536;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_switch%'
ORDER BY log_comment;

SELECT 'group by spilling';
-- `spilled_to_disk` is not about joins only: GROUP BY and ORDER BY report themselves through the
-- same temporary data scopes.
SELECT a, count() FROM (SELECT number AS a FROM numbers(100000)) GROUP BY a
FORMAT Null
SETTINGS log_comment = '04891_join_count_group_by_spill', max_bytes_before_external_group_by = 1000000, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1000;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_group\_by\_spill%'
ORDER BY log_comment;

SELECT 'order by spilling';
SELECT a FROM (SELECT number AS a FROM numbers(100000)) ORDER BY a
FORMAT Null
SETTINGS log_comment = '04891_join_count_order_by_spill', max_bytes_before_external_sort = 100000, max_bytes_ratio_before_external_sort = 0;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04891\_join\_count\_order\_by\_spill%'
ORDER BY log_comment;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE tj;
DROP TABLE ta;
DROP TABLE tb;
DROP TABLE m1;
DROP TABLE m2;
