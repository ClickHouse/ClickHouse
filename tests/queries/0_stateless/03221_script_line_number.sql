SET log_comment = 'script_line_number_test', log_queries = 1;

CREATE TABLE 03221_t  (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE 03221_t1 (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE 03221_t2 (n Int8) ENGINE=MergeTree ORDER BY n;

INSERT INTO 03221_t SELECT * FROM numbers(10);

-- test if line breaking works
INSERT INTO 03221_t1
    SELECT* FROM numbers(10);

-- test if double line break works
INSERT INTO 03221_t2

    SELECT * FROM numbers(10);

DROP TABLE 03221_t;
DROP TABLE 03221_t1;
DROP TABLE 03221_t2;

SYSTEM FLUSH LOGS;
SELECT script_line_number FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = 'script_line_number_test' AND event_date >= yesterday() AND type = 1 ORDER BY event_time_microseconds ASC LIMIT 11;
