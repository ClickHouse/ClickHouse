-- DISTINCT in order and a `set` skip index keyed on a single 16-byte column.
SET optimize_distinct_in_order = 1, optimize_read_in_order = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_uuid_key;
CREATE TABLE t_uuid_key (a UInt64, u UUID, INDEX idx u TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';

-- Six rows per `a`: four distinct `u` that differ pairwise in one 8-byte half or the other, then two repeats.
INSERT INTO t_uuid_key SELECT intDiv(number, 6),
    toUUID(concat(leftPad(toString(intDiv(number, 6) + 1), 8, '0'), '-0000-000', toString(number % 2), '-0000-00000000000', toString(intDiv(number % 6, 2) % 2)))
FROM numbers(3000);

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM (SELECT DISTINCT a, u FROM t_uuid_key)) WHERE explain ILIKE '%DistinctSortedStreamTransform%';
SELECT count() FROM (SELECT DISTINCT a, u FROM t_uuid_key);

SELECT count() FROM t_uuid_key WHERE u = '00000001-0000-0001-0000-000000000001'
SETTINGS force_data_skipping_indices = 'idx', use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0, max_rows_to_read = 4;

DROP TABLE t_uuid_key;
