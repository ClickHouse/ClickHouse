CREATE TABLE t1
(
    id Int32,
    fid Int32,
    tid Int32
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE t2
(
    id Int32,
    status Nullable(String),
    resource_id Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE t3
(
    id Int32,
    status String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t1 SELECT * REPLACE(1 as fid, 2 as tid) 
FROM generateRandom(1, 2, 2) LIMIT 1000;

INSERT INTO t2 SELECT * REPLACE(1 as id, 'OPEN' as status, NULL as resource_id) 
FROM generateRandom(1, 2, 2) LIMIT 1000;

INSERT INTO t3 SELECT * REPLACE('BACKLOG' as status, 2 as id) 
FROM generateRandom(1, 2, 2) LIMIT 1000;

SET enable_parallel_replicas = 0;
SET enable_analyzer = 1;

SELECT 1
FROM t2
LEFT JOIN t1 ON t2.id = t1.fid
LEFT JOIN t3 ON t1.tid = t3.id
WHERE true AND (t2.resource_id IS NOT NULL) AND (t2.status IN ('OPEN')) AND (t3.status IN ('BACKLOG'))
SETTINGS log_comment = '03594_push_more_filters_down_joins';

SYSTEM FLUSH LOGS query_log;

SELECT throwIf(ProfileEvents['JoinResultRowCount'] != 0)
FROM system.query_log
WHERE log_comment = '03594_push_more_filters_down_joins' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= NOW() - INTERVAL '10 MINUTE'
FORMAT Null;

SELECT splitByWhitespace(trimBoth(explain))[1] AS step
FROM (
    EXPLAIN
    SELECT 1
    FROM t2
    LEFT JOIN t1 ON t2.id = t1.fid
    LEFT JOIN t3 ON t1.tid = t3.id
    WHERE true AND (t2.resource_id IS NOT NULL) AND (t2.status IN ('OPEN')) AND (t3.status IN ('BACKLOG'))
    SETTINGS enable_join_runtime_filters=0
)
WHERE step ILIKE 'Join%' OR step ILIKE '%Filter%';
