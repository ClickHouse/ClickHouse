-- Regression test for WITH FILL STALENESS across chunk boundaries.
-- When data is split into multiple chunks (e.g., due to small index_granularity),
-- the staleness constraint from the last original row of the previous chunk
-- must be preserved, not overwritten by the first row of the new chunk.

SET session_timezone = 'Europe/Amsterdam';

DROP TABLE IF EXISTS with_fill_staleness_cross_chunk;
CREATE TABLE with_fill_staleness_cross_chunk (a DateTime, c UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 3;

INSERT INTO with_fill_staleness_cross_chunk
SELECT
    toDateTime('2016-06-15 23:00:00') + number * 5 AS a,
    number * 5 AS c
FROM numbers(6);

-- With index_granularity=3, the 6 rows are split into 2 granules: [0,5,10] and [15,20,25].
-- In DESC order, the reader produces 2 chunks: [25,20,15] and [10,5,0].
-- The staleness constraint from row 15 (= 13) must limit filling between 15 and 10.

SELECT 'descending with staleness across chunks';
SELECT a, c, 'original' as original
FROM with_fill_staleness_cross_chunk
ORDER BY a DESC
WITH FILL STALENESS INTERVAL -2 SECOND
INTERPOLATE (c)
SETTINGS max_block_size = 100000, max_threads = 1;

SELECT 'ascending with staleness across chunks';
SELECT a, c, 'original' as original
FROM with_fill_staleness_cross_chunk
ORDER BY a ASC
WITH FILL STALENESS INTERVAL 2 SECOND
INTERPOLATE (c)
SETTINGS max_block_size = 100000, max_threads = 1;

DROP TABLE with_fill_staleness_cross_chunk;
