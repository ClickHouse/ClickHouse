-- Tests for storing `String` columns of the hash table payload in row-major form.
-- A value of up to 15 bytes is stored inline, a longer one is copied out of line.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_mixed;
DROP TABLE IF EXISTS right_long;
DROP TABLE IF EXISTS right_sparse;
DROP TABLE IF EXISTS left_batches;
DROP TABLE IF EXISTS right_batches;

CREATE TABLE left (k Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right (k Int64, s1 String, s2 Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE right_mixed (k Int64, s1 String, s2 String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE right_long (k Int64, s1 String, s2 String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE right_sparse (k Int64, s1 Nullable(String), s2 Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE left_batches (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE right_batches (k Int64, s1 String, s2 Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;

INSERT INTO left SELECT number FROM numbers(10);
-- Empty values, and values at the 15/16 byte inline boundary. Left keys 0 to 6 and right keys 10 and 11 have no match.
INSERT INTO right VALUES (7, '', NULL), (8, repeat('a', 15), ''), (9, repeat('b', 16), repeat('c', 15)), (10, '0', repeat('d', 16)), (11, '1', '1');
INSERT INTO right VALUES (7, 'dup', NULL);
INSERT INTO right_mixed SELECT number + 7, if(number % 2 = 0, repeat('a', 20), toString(number)), toString(number) FROM numbers(5);
INSERT INTO right_long SELECT number + 7, repeat('z', 30), repeat('w', 30) FROM numbers(5);
INSERT INTO right_sparse SELECT number + 7, if(number % 10 = 0, 'abcd', NULL), if(number % 10 = 0, 'efgh', NULL) FROM numbers(10);
-- One part read in `k` order: the row of key 0, keys with short values, then keys with long values.
INSERT INTO left_batches SELECT 0;
INSERT INTO left_batches SELECT 1 + number % 999 FROM numbers(10000);
INSERT INTO left_batches SELECT 1000 + number % 100 FROM numbers(15000);
OPTIMIZE TABLE left_batches FINAL;
-- Key 0 has 10000 rows. Keys 1000 to 1099 have 64-byte values, few enough to keep the average below 15 bytes.
INSERT INTO right_batches SELECT if(number < 10000, 0, number - 9999) AS k, if(k < 1000, toString(number), rightPad(toString(number), 64, 'a')) AS s, if(number % 4 = 0, NULL, s)
FROM numbers(11099);

SET join_algorithm = 'hash';

-- Pin planner settings
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0; -- Randomized statistics carry no average sizes
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1; -- The admission of a `String` column reads its average size from the statistics
SET min_rows_ratio_for_hash_join_row_store = 0;
SET min_string_column_bytes_for_hash_join_row_store = 1;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

SELECT 'short_strings', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_short_strings';

-- Some values are longer than 15 bytes, but the average still fits.
SELECT 'mixed_strings', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right_mixed r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right_mixed r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_mixed_strings';

-- The average is too large, so the columns stay columnar.
SELECT 'long_strings', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right_long r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right_long r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_long_strings';

-- The estimated size of each `String` column is below the minimum.
SELECT 'high_min_bytes_strings', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS min_string_column_bytes_for_hash_join_row_store = 1000000, enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS min_string_column_bytes_for_hash_join_row_store = 1000000, enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_high_min_bytes_strings';

-- Only non-`NULL` rows count towards the average size of a `Nullable(String)` value, so mostly `NULL` columns are stored.
SELECT 'sparse_nullable_strings', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right_sparse r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right_sparse r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_sparse_nullable_strings';

-- Unmatched rows read back as `''` and `NULL`.
SELECT 'left', (SELECT sum(cityHash64(tuple(*))) FROM left l LEFT JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l LEFT JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_left';

SELECT 'full', (SELECT sum(cityHash64(tuple(*))) FROM left l FULL JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l FULL JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_full';

SELECT 'empty', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k WHERE r.s1 = '' SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k WHERE r.s1 = '' SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_empty';

SELECT 'boundary', (SELECT sumIf(cityHash64(tuple(*)), length(r.s1) IN (15, 16)) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sumIf(cityHash64(tuple(*)), length(r.s1) IN (15, 16)) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_boundary';

SELECT 'parallel_hash', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', max_threads = 4, enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_parallel_hash';

-- More than 3 rows in the bucket spill it, which takes the stored blocks back out of the hash table and reads the row store back into columns.
SELECT 'grace_hash_spilled', (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS join_algorithm = 'grace_hash', legacy_join_size_limits_trigger_spilling = 1, max_rows_in_join = 3, grace_hash_join_initial_buckets = 1, enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left l INNER JOIN right r ON l.k = r.k SETTINGS join_algorithm = 'grace_hash', legacy_join_size_limits_trigger_spilling = 1, max_rows_in_join = 3, grace_hash_join_initial_buckets = 1, enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_grace_hash_spilled';

-- One probe block is gathered in several batches: the rows of key 0 span a batch boundary, and the long values at the
-- end run out of the reserved chars after earlier batches were copied.
SET max_block_size = 65409, max_joined_block_size_rows = 0, max_joined_block_size_bytes = 0, joined_block_split_single_row = 0;
SELECT 'batches', (SELECT sum(cityHash64(tuple(*))) FROM left_batches l INNER JOIN right_batches r ON l.k = r.k SETTINGS enable_hash_join_row_store = 1)
    = (SELECT sum(cityHash64(tuple(*))) FROM left_batches l INNER JOIN right_batches r ON l.k = r.k SETTINGS enable_hash_join_row_store = 0)
SETTINGS log_comment = 'rs_batches';

SYSTEM FLUSH LOGS text_log, query_log;

-- Which queries built a row store.
SELECT replace(log_comment, 'rs_', ''), query_id IN (
    SELECT query_id FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND message_format_string = '{}Initialized Row store with {} columns')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND log_comment LIKE 'rs\\_%' AND type = 'QueryFinish'
ORDER BY log_comment;

SELECT 'grace_hash_spilled spilled', ProfileEvents['ExternalJoinWritePart'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'rs_grace_hash_spilled' AND type = 'QueryFinish';

DROP TABLE right_batches;
DROP TABLE left_batches;
DROP TABLE right_sparse;
DROP TABLE right_long;
DROP TABLE right_mixed;
DROP TABLE right;
DROP TABLE left;
