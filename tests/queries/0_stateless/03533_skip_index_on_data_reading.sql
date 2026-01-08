-- Tags: no-parallel-replicas
-- no-parallel-replicas: use_skip_indexes_on_data_read is not supported with parallel replicas.

-- { echo ON }

SET use_skip_indexes_on_data_read = 1;
SET max_rows_to_read = 0;

set use_query_condition_cache=0;
set merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    id UInt64,
    event_date Date,
    user_id UInt32,
    url String,
    region String,
    INDEX region_idx region TYPE minmax GRANULARITY 1,
    INDEX user_id_idx user_id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS
    index_granularity = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

-- create 3 parts to test concurrent processing.
INSERT INTO test VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe'), (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west'), (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west'), (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west'), (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

INSERT INTO test VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe'), (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west'), (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west'), (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west'), (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

INSERT INTO test VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe'), (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west'), (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west'), (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west'), (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

-- disable move to PREWHERE to ensure RowsReadByPrewhereReaders and RowsReadByMainReader reflect actual filtering on read behavior for testing
SET optimize_move_to_prewhere = 0;

-- agree on one granule
SELECT * FROM test WHERE region = 'europe' AND user_id = 101 ORDER BY ALL SETTINGS log_comment = 'test_1';

-- all filtered
SELECT * FROM test WHERE region = 'unknown' AND user_id = 101 ORDER BY ALL SETTINGS log_comment = 'test_2';

-- narrowing filter via user_id_idx
SELECT * FROM test WHERE region = 'us_west' AND user_id = 106 ORDER BY ALL SETTINGS log_comment = 'test_3';

-- test with an OR filter - 3 rows/granules for user_id=101 union 3 rows/granules for 'asia'
SELECT * FROM test WHERE region = 'asia' OR user_id = 101 ORDER BY ALL SETTINGS log_comment = 'test_4';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_1';

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_2';

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_3';

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_4';

DROP TABLE test;

-- check partially materialized index, it should only affect related parts

DROP TABLE IF EXISTS test_partial_index;

CREATE TABLE test_partial_index
(
    id UInt64,
    event_date Date,
    user_id UInt32,
    url String,
    region String
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS
    index_granularity = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

-- insert a part with no index
INSERT INTO test_partial_index VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe'), (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west'), (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west'), (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west'), (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

ALTER TABLE test_partial_index ADD INDEX region_idx region TYPE minmax GRANULARITY 1;

-- insert a part with region index
INSERT INTO test_partial_index VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe'), (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west'), (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west'), (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west'), (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

ALTER TABLE test_partial_index ADD INDEX user_id_idx user_id TYPE minmax GRANULARITY 1;

-- insert a part with user_id index
INSERT INTO test_partial_index VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe'), (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west'), (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west'), (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west'), (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

-- agree on one granule
SELECT * FROM test_partial_index WHERE region = 'europe' AND user_id = 101 ORDER BY ALL SETTINGS log_comment = 'test_partial_1';

-- all filtered
SELECT * FROM test_partial_index WHERE region = 'unknown' AND user_id = 101 ORDER BY ALL SETTINGS log_comment = 'test_partial_2';

-- narrowing filter via user_id_idx
SELECT * FROM test_partial_index WHERE region = 'us_west' AND user_id = 106 ORDER BY ALL SETTINGS log_comment = 'test_partial_3';

-- Skip indexes on OR supported.
-- All 5 rows from part1 (no skip indexes) +
-- All 5 rows from part2 (because no index on user_id) +
-- 2 rows from part3 -> 1 row each for region='asia' and user_id=101.
-- Total 12
SELECT * FROM test_partial_index WHERE region = 'asia' OR user_id = 101 ORDER BY ALL SETTINGS log_comment = 'test_partial_4';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_partial_1';

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_partial_2';

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_partial_3';

SELECT ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_partial_4';

DROP TABLE test_partial_index;
