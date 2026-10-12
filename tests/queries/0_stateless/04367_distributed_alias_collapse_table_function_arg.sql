-- Tags: distributed, no-fasttest
-- Tag no-fasttest: Depends on AWS (uses the `oss`/`s3` table function against the test MinIO)

-- Regression test for the LOGICAL_ERROR "Invalid action query tree node ..." exception.
-- When a Distributed query deduplicates structurally-identical duplicate-ALIAS columns, the
-- shard-collapse reconstruction walks the whole shard query tree to translate action names. It
-- must NOT descend into table-function argument lists: a `key = value` named-collection override
-- (e.g. `oss(s3_conn, filename = '...')`) leaves `key` as an unresolved identifier that
-- `calculateActionNodeName` cannot name, which previously threw a `LOGICAL_ERROR` exception.

DROP TABLE IF EXISTS local_tf_arg;
DROP TABLE IF EXISTS dist_tf_arg;

CREATE TABLE local_tf_arg
(
    x UInt8,
    a1 String ALIAS toString(x),
    a2 String ALIAS toString(x)
)
ENGINE = MergeTree ORDER BY x;

CREATE TABLE dist_tf_arg AS local_tf_arg
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_tf_arg, rand());

INSERT INTO local_tf_arg (x) VALUES (7);

SET enable_analyzer = 1;

-- The duplicate ALIAS columns a1/a2 collapse to a single shard column, triggering the
-- shard-collapse reconstruction. The right side GLOBAL-joins a table function whose arguments
-- contain a `filename = '...'` named-collection override (an unresolved identifier). The wildcard
-- matches no object, so the result is empty; the point is that planning must not throw a `LOGICAL_ERROR` exception.
SELECT a1, a2, count() AS c FROM dist_tf_arg
    GLOBAL NATURAL FULL OUTER JOIN oss(s3_conn, filename = '04367_no_such_object_*.csv', format = 'CSV', structure = 'p String, q String') AS j
    GROUP BY a1, a2 ORDER BY a1;

DROP TABLE dist_tf_arg;
DROP TABLE local_tf_arg;
