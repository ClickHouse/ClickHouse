set log_queries=1;
set log_queries_min_type='QUERY_FINISH';
set optimize_use_implicit_projections=1;

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    `id` UInt64,
    `id2` UInt64,
    `id3` UInt64,
    PROJECTION t_normal
    (
        SELECT
            id,
            id2,
            id3
        ORDER BY
            id2,
            id,
            id3
    ),
    PROJECTION t_agg
    (
        SELECT
            sum(id3)
        GROUP BY id2
    )
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8;

insert into t SELECT number, -number, number FROM numbers(10000);

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;
SELECT * FROM t WHERE id2 = 3 FORMAT Null;
SELECT sum(id3) FROM t GROUP BY id2 FORMAT Null;
SELECT min(id) FROM t FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    --Remove the prefix string which is a mutable database name.
    arrayStringConcat(arrayPopFront(splitByString('.', projections[1])), '.')
FROM
    system.query_log
WHERE
    current_database=currentDatabase() and query = 'SELECT * FROM t WHERE id2 = 3 FORMAT Null;';

SELECT
    --Remove the prefix string which is a mutable database name.
    arrayStringConcat(arrayPopFront(splitByString('.', projections[1])), '.')
FROM
    system.query_log
WHERE
    current_database=currentDatabase() and query = 'SELECT sum(id3) FROM t GROUP BY id2 FORMAT Null;';

SELECT
    --Remove the prefix string which is a mutable database name.
    arrayStringConcat(arrayPopFront(splitByString('.', projections[1])), '.')
FROM
    system.query_log
WHERE
    current_database=currentDatabase() and query = 'SELECT min(id) FROM t FORMAT Null;';

DROP TABLE t;
