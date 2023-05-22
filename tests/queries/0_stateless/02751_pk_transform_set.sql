SET transform_set_with_monotonic_functions_for_analysis = 1;
DROP TABLE IF EXISTS events;
CREATE TABLE events (ts DateTime, e String) ENGINE=MergeTree() ORDER BY toStartOfInterval(ts, toIntervalSecond(5)) SETTINGS index_granularity = 1;
INSERT INTO events VALUES ('2023-06-10 00:00:01', 'x') ('2023-06-10 00:00:07', 'y') ('2023-06-10 00:00:13', 'z');
SELECT * FROM events WHERE ts IN ('2023-06-10 00:00:01') SETTINGS force_primary_key = 1, max_rows_to_read = 1;