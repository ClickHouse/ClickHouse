-- Tests for storing `String` columns of the hash table payload in row-major form.
-- A value of up to 15 bytes is stored inline, a longer one is copied out of line.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_mixed;
DROP TABLE IF EXISTS right_long;

CREATE TABLE left (k Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right (k Int64, s1 String, s2 Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE right_mixed (k Int64, s1 String, s2 String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE right_long (k Int64, s1 String, s2 String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;

INSERT INTO left SELECT number FROM numbers(10);
INSERT INTO right SELECT number + 7, toString(number), if(number = 3, NULL, toString(number)) FROM numbers(5);
INSERT INTO right VALUES (7, 'dup', NULL);
INSERT INTO right_mixed SELECT number + 7, if(number % 2 = 0, repeat('a', 20), toString(number)), toString(number) FROM numbers(5);
INSERT INTO right_long SELECT number + 7, repeat('z', 30), repeat('w', 30) FROM numbers(5);

SET join_algorithm = 'hash';

-- Pin planner settings
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0; -- Randomized statistics carry no average sizes
SET use_statistics = 1; -- The admission of a `String` column reads its average size from the statistics
SET min_rows_ratio_for_hash_join_row_store = 0;
SET min_string_column_bytes_for_hash_join_row_store = 0;

SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL
SETTINGS log_comment = 'rs_short_strings';

-- Some values are longer than 15 bytes, but the average still fits.
SELECT * FROM left l INNER JOIN right_mixed r ON l.k = r.k ORDER BY ALL
SETTINGS log_comment = 'rs_mixed_strings';

SELECT * FROM left l INNER JOIN right_long r ON l.k = r.k ORDER BY ALL
SETTINGS log_comment = 'rs_long_strings';

-- The estimated size of each `String` column is below the minimum.
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL
SETTINGS log_comment = 'rs_gated_strings', min_string_column_bytes_for_hash_join_row_store = 1000000;

SYSTEM FLUSH LOGS text_log, query_log;

SELECT 'rs_short_strings', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_short_strings' AND type = 'QueryFinish');

SELECT 'rs_mixed_strings', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_mixed_strings' AND type = 'QueryFinish');

SELECT 'rs_long_strings', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_long_strings' AND type = 'QueryFinish');

SELECT 'rs_gated_strings', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_gated_strings' AND type = 'QueryFinish');

DROP TABLE right_long;
DROP TABLE right_mixed;
DROP TABLE right;
DROP TABLE left;
