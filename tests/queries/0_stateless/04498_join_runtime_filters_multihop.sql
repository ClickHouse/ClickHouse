-- Tests for enable_join_runtime_filters_index_analysis with filter
-- propogation across multiple tables in the join

DROP TABLE IF EXISTS mh_fact;
DROP TABLE IF EXISTS mh_d1;
DROP TABLE IF EXISTS mh_d2;
DROP TABLE IF EXISTS mh_d3;

CREATE TABLE mh_fact (dk UInt64, v UInt64) ENGINE = MergeTree ORDER BY dk SETTINGS index_granularity = 16;
CREATE TABLE mh_d1 (dk UInt64, k1 UInt64) ENGINE = MergeTree ORDER BY dk;
CREATE TABLE mh_d2 (k1 UInt64, k2 UInt64) ENGINE = MergeTree ORDER BY k1;
CREATE TABLE mh_d3 (k2 UInt64, tag String) ENGINE = MergeTree ORDER BY k2;

INSERT INTO mh_fact SELECT number, number FROM numbers(2000);
INSERT INTO mh_d1 SELECT number, number FROM numbers(2000);
INSERT INTO mh_d2 SELECT number, number FROM numbers(2000);
INSERT INTO mh_d3 SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
-- Left-side join pruning is intentionally disabled under parallel replicas; pin PR off so the
-- multi-hop pruning assertions are exercised (the ParallelReplicas CI job otherwise forces it on).
SET enable_parallel_replicas = 0;

-- Pin the join order so the chained runtime filters reliably reach mh_fact (otherwise the
-- optimizer may build the fact side and the transferred filter never lands on its primary key).
SET query_plan_join_swap_table = true;
SET allow_statistics_optimize = 0;
-- Runtime filters are only built for hash-family joins; pin the algorithm so a randomized
-- partial_merge/full_sorting_merge (which build no runtime filter) does not disable pruning.
SET join_algorithm = 'hash';
-- Disable cost-based join reordering so the syntactic chain order is preserved; otherwise the
-- reorderer can rearrange the tables and the transferred filter never reaches mh_fact.
SET query_plan_optimize_join_order_limit = 0;

-- 2-hop: the predicate on mh_d2 is transferred mh_d2 -> mh_d1 -> mh_fact and prunes mh_fact by its primary key.
SELECT count(), sum(f.v)
FROM mh_fact AS f
INNER JOIN mh_d1 ON f.dk = mh_d1.dk
INNER JOIN mh_d2 ON mh_d1.k1 = mh_d2.k1
WHERE mh_d2.k2 < 64
FORMAT Null
SETTINGS log_comment = '04498_mh_2hop';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04498_mh_2hop' AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

-- 3-hop: the predicate on mh_d3 is transferred mh_d3 -> mh_d2 -> mh_d1 -> mh_fact.
SELECT count(), sum(f.v)
FROM mh_fact AS f
INNER JOIN mh_d1 ON f.dk = mh_d1.dk
INNER JOIN mh_d2 ON mh_d1.k1 = mh_d2.k1
INNER JOIN mh_d3 ON mh_d2.k2 = mh_d3.k2
WHERE mh_d3.tag = 'hot'
FORMAT Null
SETTINGS log_comment = '04498_mh_3hop';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04498_mh_3hop' AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1;

-- Correctness: the transferred filter must never change results. Verify feature off == on.
SELECT
    (SELECT sum(f.v) FROM mh_fact AS f INNER JOIN mh_d1 ON f.dk = mh_d1.dk INNER JOIN mh_d2 ON mh_d1.k1 = mh_d2.k1 WHERE mh_d2.k2 < 64 SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT sum(f.v) FROM mh_fact AS f INNER JOIN mh_d1 ON f.dk = mh_d1.dk INNER JOIN mh_d2 ON mh_d1.k1 = mh_d2.k1 WHERE mh_d2.k2 < 64 SETTINGS enable_join_runtime_filters_index_analysis = 1);
SELECT
    (SELECT sum(f.v) FROM mh_fact AS f INNER JOIN mh_d1 ON f.dk = mh_d1.dk INNER JOIN mh_d2 ON mh_d1.k1 = mh_d2.k1 INNER JOIN mh_d3 ON mh_d2.k2 = mh_d3.k2 WHERE mh_d3.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT sum(f.v) FROM mh_fact AS f INNER JOIN mh_d1 ON f.dk = mh_d1.dk INNER JOIN mh_d2 ON mh_d1.k1 = mh_d2.k1 INNER JOIN mh_d3 ON mh_d2.k2 = mh_d3.k2 WHERE mh_d3.tag = 'hot' SETTINGS enable_join_runtime_filters_index_analysis = 1);

DROP TABLE mh_fact;
DROP TABLE mh_d1;
DROP TABLE mh_d2;
DROP TABLE mh_d3;
