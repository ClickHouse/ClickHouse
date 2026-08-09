-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Regression test for the vector-search `ORDER BY <distance> LIMIT` rewrite with `arrayJoin` below the sort.
--
-- `arrayJoin` changes the number of rows: an empty array drops its base row. The rewrite shortlists a few
-- base rows through the vector similarity index and feeds only those to the expression, so the rows a later
-- base row would have contributed are never produced and the query returns fewer rows than the `LIMIT` -
-- here nothing at all instead of 16. `optimizeTopK` already rejects `arrayJoin` for the same reason (#82279);
-- the vector-search rewrites did not.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_04813;

CREATE TABLE t_04813 (id UInt32, tags Array(UInt32), vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- The 16 rows nearest to [0, 1] have empty `tags`, so the first value `arrayJoin` can produce is 16.
INSERT INTO t_04813 SELECT number, if(number < 16, [], [number]), [toFloat32(number), toFloat32(number + 1)]
FROM numbers(64);

SELECT arrayJoin(tags) FROM t_04813 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1;

SELECT '--';

-- Control: the same query without the index.
SELECT arrayJoin(tags) FROM t_04813 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1 SETTINGS use_skip_indexes = 0;

DROP TABLE t_04813;
