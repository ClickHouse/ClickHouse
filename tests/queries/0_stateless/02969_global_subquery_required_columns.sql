#!/usr/bin/env -S ${HOME}/clickhouse-client --progress --queries-file

DROP TABLE IF EXISTS tids;
CREATE TABLE tids
(
    key Int32,
    tid Int32,
    cid Int32,
    iid Int32
) ENGINE = MergeTree() ORDER BY key;
INSERT INTO tids VALUES (130, 1, 1, 1);

DROP TABLE IF EXISTS rd;
CREATE TABLE rd (rid Int32, ts DateTime, domain String) ENGINE = MergeTree() ORDER BY rid;
INSERT INTO rd VALUES (1, '2021-01-01 00:00:00', 'example.com');

-- with analyzer: 'DB::Exception: JOINs are not supported with parallel replicas. (SUPPORT_IS_DISABLED)'
SET allow_experimental_analyzer = 0;

SET use_hedged_requests = 0;

-- parallel replicas may produce 'ConnectionPoolWithFailover: Connection failed at try ...'
SET send_logs_level = 'error';

SELECT
    tid,
    domain
FROM
    (
        SELECT
            domain,
            ts,
            i.cid AS cid
        FROM rd AS r
        ANY INNER JOIN tids AS i ON i.iid = r.rid
    ) AS r
INNER JOIN tids AS i ON i.cid = r.cid
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'parallel_replicas'
;

