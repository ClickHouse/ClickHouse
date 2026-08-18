-- The quantized-codes vector search rewrite and `arrayJoin` below the sort.
--
-- On the vector-similarity-index path an `arrayJoin` below the sort is rejected (see
-- `04813_vector_search_array_join`): the index prunes rows inside the reader, so a base row whose `arrayJoin`
-- would have produced the surviving rows is never read and the query returns fewer rows than the `LIMIT`.
--
-- The quantized-codes path does not have that problem, and this test pins the invariant that makes it safe: its
-- shortlist `LIMIT` is spliced ABOVE the whole Expression/Filter chain, so the chain - including an `arrayJoin`
-- in `WHERE` - runs first and the shortlist only ever truncates already-expanded rows. The row count therefore
-- always reaches the `LIMIT`, even with `vector_search_index_fetch_multiplier = 1`. (An `arrayJoin` in the
-- rescore expression above the shortlist is a different matter and is rejected by `hasArrayJoin` there.)

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_04893;

CREATE TABLE t_04893
(
    id UInt32,
    tags Array(UInt32),
    vec Array(Float32) CODEC(Quantized('int8', 2))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- The 16 rows nearest to [0, 1] have empty `tags`, so `arrayJoin` drops exactly the rows a truncating shortlist
-- would have kept.
INSERT INTO t_04893 SELECT number, if(number < 16, [], [number]), [toFloat32(number), toFloat32(number + 1)]
FROM numbers(64);

-- The rewrite engages, and the filter sits below the shortlist limit. `EXPLAIN`
-- renders parents before children, so the shortlist must precede the filter.
SELECT 'shortlist_above_filter',
    arrayFirstIndex(line -> line ILIKE '%quantized shortlist limit%', groupArray(explain))
        < arrayFirstIndex(line -> line ILIKE '%Filter%', groupArray(explain))
FROM
(
    EXPLAIN SELECT id FROM t_04893 WHERE arrayJoin(tags) >= 0
    ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 4 SETTINGS vector_search_index_fetch_multiplier = 1
);

-- The shortlist never starves the result: the full `LIMIT` is returned even at multiplier 1.
SELECT 'rows_returned', count()
FROM (SELECT id FROM t_04893 WHERE arrayJoin(tags) >= 0
      ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 4 SETTINGS vector_search_index_fetch_multiplier = 1);

DROP TABLE t_04893;
