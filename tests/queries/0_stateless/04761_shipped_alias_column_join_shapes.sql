-- Tags: shard

-- The join shapes and dispatch modes for an `ALIAS` column of a shipped table expression; the type and
-- clause matrix is in 04757, and this is split off it to keep each test's runtime down.
-- Inlining must not stamp the column's name on the inlined body outside the projection, or two `JOIN`
-- sides declaring a same-named `ALIAS` column put one alias on two bodies and the remote side throws
-- `MULTIPLE_EXPRESSIONS_FOR_ALIAS`.

-- The shipping path under test only exists in the analyzer.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_join_alias;
DROP TABLE IF EXISTS t_join_plain;
DROP TABLE IF EXISTS t_join_other;
DROP TABLE IF EXISTS t_join_arr;

CREATE TABLE t_join_alias (k UInt32, v Int64, a_v Int64 ALIAS v * 2, a_narrow Int32 ALIAS v + 1, a_common Int64 ALIAS v + 1)
    ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_join_plain (k UInt32, w Int64) ENGINE = MergeTree ORDER BY k;
-- Declares `a_common` too, with a different body, to cover the same-name-on-both-sides case.
CREATE TABLE t_join_other (k UInt32, w Int64, a_common Int64 ALIAS w + 2) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_join_arr (k UInt32, arr Array(Int64), a_k Int64 ALIAS k * 3) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_join_alias SELECT number, number FROM numbers(30);
INSERT INTO t_join_plain SELECT number * 2, number FROM numbers(15);
INSERT INTO t_join_other SELECT number, number FROM numbers(30);
INSERT INTO t_join_arr SELECT number, [number, number + 1] FROM numbers(20);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Pin what the test runner randomizes, otherwise these queries silently move off the path under test.
SET automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

SELECT 'same-named ALIAS column on both sides';

SELECT l.a_common, r.a_common FROM t_join_alias AS l GLOBAL INNER JOIN t_join_other AS r ON l.k = r.k ORDER BY ALL LIMIT 3;
SELECT l.a_v, r.a_v FROM t_join_alias AS l GLOBAL INNER JOIN t_join_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'shipped through ARRAY JOIN and USING';

SELECT r.a_v FROM t_join_arr AS x ARRAY JOIN x.arr GLOBAL INNER JOIN t_join_alias AS r ON x.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT x.a_k FROM t_join_arr AS x ARRAY JOIN x.arr GLOBAL RIGHT JOIN t_join_plain AS p ON x.k = p.k ORDER BY ALL DESC LIMIT 3;
-- A `JOIN USING` key list records how the key resolves per side and must survive inlining untouched.
SELECT k FROM t_join_alias GLOBAL INNER JOIN t_join_plain USING (k) ORDER BY ALL DESC LIMIT 3;
SELECT a_v FROM t_join_alias GLOBAL INNER JOIN t_join_plain USING (k) ORDER BY ALL DESC LIMIT 3;
SELECT l.a_v FROM t_join_alias AS l GLOBAL RIGHT JOIN t_join_plain AS p ON l.k = p.k ORDER BY ALL DESC LIMIT 3;

SELECT 'right-side join kinds with a local join declined';

SET parallel_replicas_prefer_local_join = 0;
SELECT l.a_v FROM t_join_alias AS l RIGHT ANTI JOIN t_join_plain AS p ON l.k = p.k ORDER BY ALL LIMIT 3;
SELECT l.a_v FROM t_join_alias AS l RIGHT SEMI JOIN t_join_plain AS p ON l.k = p.k ORDER BY ALL DESC LIMIT 3;
SET parallel_replicas_prefer_local_join = 1;

SELECT 'query shipped as SQL instead of a plan';

SET parallel_replicas_local_plan = 0;
SELECT r.a_v FROM t_join_alias AS l GLOBAL INNER JOIN t_join_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT l.a_common, r.a_common FROM t_join_alias AS l GLOBAL INNER JOIN t_join_other AS r ON l.k = r.k ORDER BY ALL LIMIT 3;
SELECT r.a_narrow, toTypeName(r.a_narrow) FROM t_join_alias AS l GLOBAL INNER JOIN t_join_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'same-named ALIAS column on both sides, shipped by remote()';

-- The same shape on the Distributed path, which has always inlined and so has always put one alias on
-- two different bodies here.
SET enable_parallel_replicas = 0;
SELECT l.a_common, r.a_common
FROM remote('127.0.0.1', currentDatabase(), t_join_alias) AS l
GLOBAL INNER JOIN remote('127.0.0.1', currentDatabase(), t_join_other) AS r ON l.k = r.k
ORDER BY ALL LIMIT 3;

SELECT l.a_common FROM remote('127.0.0.1', currentDatabase(), t_join_alias) AS l
GLOBAL INNER JOIN remote('127.0.0.1', currentDatabase(), t_join_other) AS r ON l.k = r.k
WHERE r.a_common > 2 ORDER BY ALL LIMIT 3;

DROP TABLE t_join_arr;
DROP TABLE t_join_other;
DROP TABLE t_join_plain;
DROP TABLE t_join_alias;
