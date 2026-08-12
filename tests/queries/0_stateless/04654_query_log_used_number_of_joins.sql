-- `used_number_of_joins` counts the physical joins of the executed pipeline, `used_join_algorithms`,
-- `used_join_kinds` and `used_join_strictness` describe them, and `join_spilled_to_disk` tells
-- whether any of them wrote temporary data. The cases that do not force an algorithm are checked
-- with both analyzers: they build the join steps in different places.
--
-- The algorithm is set explicitly wherever it is asserted, because the choice among the algorithms
-- allowed by `join_algorithm` is made at run time and depends on the number of threads.

SET log_queries = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS tj;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;

CREATE TABLE t1 (a UInt64) ENGINE = Memory;
CREATE TABLE t2 (a UInt64) ENGINE = Memory;
CREATE TABLE t3 (a UInt64) ENGINE = Memory;
CREATE TABLE tj (a UInt64, b UInt64) ENGINE = Join(ANY, LEFT, a);
CREATE TABLE ta (a UInt64, t UInt64) ENGINE = Memory;
CREATE TABLE tb (a UInt64, t UInt64) ENGINE = Memory;

INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number FROM numbers(10);
INSERT INTO t3 SELECT number FROM numbers(10);
INSERT INTO tj SELECT number, number FROM numbers(10);
INSERT INTO ta SELECT number, number * 2 FROM numbers(10);
INSERT INTO tb SELECT number, number FROM numbers(10);

-- No join at all.
SELECT count() FROM t1 FORMAT Null SETTINGS log_comment = '04654_join_count_none_new', enable_analyzer = 1;
SELECT count() FROM t1 FORMAT Null SETTINGS log_comment = '04654_join_count_none_old', enable_analyzer = 0;

-- A single INNER JOIN. The strictness is not written, so it comes from `join_default_strictness`.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04654_join_count_inner_new', enable_analyzer = 1, join_algorithm = 'hash';
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04654_join_count_inner_old', enable_analyzer = 0, join_algorithm = 'hash';

-- Three tables, so two physical joins. The arrays hold distinct values, so they report a single
-- element each while the count is 2.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a FORMAT Null SETTINGS log_comment = '04654_join_count_two_new', enable_analyzer = 1, join_algorithm = 'hash';
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a FORMAT Null SETTINGS log_comment = '04654_join_count_two_old', enable_analyzer = 0, join_algorithm = 'hash';

-- CROSS JOIN, for which the strictness is meaningless. It has no `ON` clause, so its condition is
-- a constant and it is executed by `ConstantJoin` rather than by the requested `hash` algorithm.
-- That is why every join reports an algorithm of its own: otherwise a plain CROSS JOIN would show
-- one executed join and an empty list of algorithms.
SELECT count() FROM t1, t2 FORMAT Null SETTINGS log_comment = '04654_join_count_cross_new', enable_analyzer = 1, join_algorithm = 'hash';
SELECT count() FROM t1, t2 FORMAT Null SETTINGS log_comment = '04654_join_count_cross_old', enable_analyzer = 0, join_algorithm = 'hash';

-- A table of the Join engine is joined by `FilledJoinStep` and not by `JoinStep`.
SELECT count() FROM t1 ANY LEFT JOIN tj USING (a) FORMAT Null SETTINGS log_comment = '04654_join_count_filled_new', enable_analyzer = 1;
SELECT count() FROM t1 ANY LEFT JOIN tj USING (a) FORMAT Null SETTINGS log_comment = '04654_join_count_filled_old', enable_analyzer = 0;

-- ASOF is a strictness and not a kind, so it is reported in `used_join_strictness`.
SELECT count() FROM ta ASOF LEFT JOIN tb USING (a, t) FORMAT Null SETTINGS log_comment = '04654_join_count_asof_new', enable_analyzer = 1, join_algorithm = 'hash';
SELECT count() FROM ta ASOF LEFT JOIN tb USING (a, t) FORMAT Null SETTINGS log_comment = '04654_join_count_asof_old', enable_analyzer = 0, join_algorithm = 'hash';

-- PASTE JOIN is not one of the `join_algorithm` values, but it still reports itself, so that a
-- query whose only join is a paste join does not show an empty list of algorithms.
SELECT count() FROM (SELECT number AS a FROM numbers(10)) p1 PASTE JOIN (SELECT number AS a FROM numbers(10)) p2 FORMAT Null SETTINGS log_comment = '04654_join_count_paste_new', enable_analyzer = 1;
SELECT count() FROM (SELECT number AS a FROM numbers(10)) p1 PASTE JOIN (SELECT number AS a FROM numbers(10)) p2 FORMAT Null SETTINGS log_comment = '04654_join_count_paste_old', enable_analyzer = 0;

-- `full_sorting_merge` keeps everything in memory here.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04654_join_count_fsm', join_algorithm = 'full_sorting_merge';

-- `grace_hash` with many buckets writes the buckets it is not currently joining to disk.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) g1 JOIN (SELECT number AS a FROM numbers(10000)) g2 ON g1.a = g2.a
FORMAT Null
SETTINGS log_comment = '04654_join_count_grace_spill', join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32, grace_hash_join_max_buckets = 32;

-- `partial_merge` over its memory limit sorts the right table into temporary files.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) m1 JOIN (SELECT number AS a FROM numbers(10000)) m2 ON m1.a = m2.a
FORMAT Null
SETTINGS log_comment = '04654_join_count_merge_spill', join_algorithm = 'partial_merge', default_max_bytes_in_join = 0, max_bytes_in_join = 1024;

-- A hash join that exceeds `max_bytes_before_external_join` is replaced by `grace_hash` while the
-- query is already running, so both algorithms are reported for the one join. The threshold has to
-- be well above zero: `grace_hash` keeps doubling the number of buckets to get each of them under
-- the threshold, and a threshold of a few bytes is unreachable, so it hits
-- `grace_hash_join_max_buckets` and the query fails instead.
SELECT count() FROM (SELECT number AS a FROM numbers(10000)) s1 JOIN (SELECT number AS a FROM numbers(10000)) s2 ON s1.a = s2.a
FORMAT Null
SETTINGS log_comment = '04654_join_count_switch', join_algorithm = 'hash', max_bytes_before_external_join = 65536;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, join_spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04654\_join\_count\_%'
ORDER BY log_comment;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE tj;
DROP TABLE ta;
DROP TABLE tb;
