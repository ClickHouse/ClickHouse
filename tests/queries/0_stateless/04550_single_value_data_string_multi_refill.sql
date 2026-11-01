-- A valid AggregateFunction(max, String) state larger than a single compressed block spans
-- multiple read-buffer refills when read back from a table. Check that it round-trips
-- unchanged through SingleValueDataString::read - the other tests for that function
-- (02477, 02481) only read states that fit into a single buffer.

DROP TABLE IF EXISTS t_single_value_multi_refill;
CREATE TABLE t_single_value_multi_refill (x AggregateFunction(max, String)) ENGINE = MergeTree ORDER BY tuple();

-- Small compressed blocks guarantee that the 8 MB state spans many refills on read,
-- independent of the default buffer sizes.
INSERT INTO t_single_value_multi_refill
SELECT maxState(materialize(repeat('clickhouse', 800000)))
SETTINGS min_compress_block_size = 65536, max_compress_block_size = 65536;

SELECT length(maxMerge(x)) = 8000000, maxMerge(x) = repeat('clickhouse', 800000) FROM t_single_value_multi_refill;

DROP TABLE t_single_value_multi_refill;
