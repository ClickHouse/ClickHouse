-- A query can read from more than one `Merge` source. A replica designates one of them for
-- coordinated reading, and the other one is read by every replica in full, as any other
-- non-designated leaf. The designation of the initiator and the designation of a replica are
-- compared per query and not per leaf, so that planning the non-designated sibling does not look
-- like a disagreement about the designated leaf, which fails the query closed.
-- https://github.com/ClickHouse/ClickHouse/issues/67770

DROP TABLE IF EXISTS t_two_merge_l_1;
DROP TABLE IF EXISTS t_two_merge_l_2;
DROP TABLE IF EXISTS t_two_merge_r_1;
DROP TABLE IF EXISTS t_two_merge_r_2;
DROP TABLE IF EXISTS t_two_merge_l;
DROP TABLE IF EXISTS t_two_merge_r;

CREATE TABLE t_two_merge_l_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_two_merge_l_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_two_merge_r_1 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_two_merge_r_2 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;

INSERT INTO t_two_merge_l_1 SELECT number, number * 2 FROM numbers(500);
INSERT INTO t_two_merge_l_2 SELECT number + 500, number FROM numbers(500);
INSERT INTO t_two_merge_r_1 SELECT number, number * 3 FROM numbers(500);
INSERT INTO t_two_merge_r_2 SELECT number + 500, number * 4 FROM numbers(500);

CREATE TABLE t_two_merge_l ENGINE = Merge(currentDatabase(), '^t_two_merge_l_[12]$');
CREATE TABLE t_two_merge_r ENGINE = Merge(currentDatabase(), '^t_two_merge_r_[12]$');

SET enable_analyzer = 1;
SET max_threads = 4;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET automatic_parallel_replicas_mode = 0;

-- Slow the initiator's local reads so that the remote replicas actually plan and read; rows read
-- both locally and remotely would surface as wrong aggregates.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT '-- merge() INNER JOIN merge()';
SELECT count(), sum(l.v), sum(r.w)
FROM merge(currentDatabase(), '^t_two_merge_l_[12]$') AS l
INNER JOIN merge(currentDatabase(), '^t_two_merge_r_[12]$') AS r ON l.k = r.k;

SELECT '-- Merge table INNER JOIN a Merge table';
SELECT count(), sum(l.v), sum(r.w) FROM t_two_merge_l AS l INNER JOIN t_two_merge_r AS r ON l.k = r.k;

SELECT '-- merge() INNER JOIN a Merge table';
SELECT count(), sum(l.v), sum(r.w)
FROM merge(currentDatabase(), '^t_two_merge_l_[12]$') AS l
INNER JOIN t_two_merge_r AS r ON l.k = r.k;

SELECT '-- two merge() sources of the same tables joined with each other';
SELECT count(), sum(l.v), sum(r.v)
FROM merge(currentDatabase(), '^t_two_merge_l_[12]$') AS l
INNER JOIN merge(currentDatabase(), '^t_two_merge_l_[12]$') AS r ON l.k = r.k;

SELECT '-- merge() joined with a subquery over another merge()';
SELECT sum(cnt), sum(s)
FROM (
    SELECT l.k % 10 AS g, count() AS cnt, sum(r.w) AS s
    FROM merge(currentDatabase(), '^t_two_merge_l_[12]$') AS l
    INNER JOIN (SELECT k, w FROM merge(currentDatabase(), '^t_two_merge_r_[12]$')) AS r ON l.k = r.k
    GROUP BY g);

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_two_merge_r;
DROP TABLE t_two_merge_l;
DROP TABLE t_two_merge_r_2;
DROP TABLE t_two_merge_r_1;
DROP TABLE t_two_merge_l_2;
DROP TABLE t_two_merge_l_1;
