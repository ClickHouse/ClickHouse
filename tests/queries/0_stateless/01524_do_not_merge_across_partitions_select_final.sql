DROP TABLE IF EXISTS select_final;

SET do_not_merge_across_partitions_select_final = 1;
SET max_threads = 0;

CREATE TABLE select_final (t DateTime, x Int32, string String) ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(t) ORDER BY (x, t);

INSERT INTO select_final SELECT toDate('2000-01-01'), number, '' FROM numbers(2);
INSERT INTO select_final SELECT toDate('2000-01-01'), number + 1, '' FROM numbers(2);

INSERT INTO select_final SELECT toDate('2020-01-01'), number, '' FROM numbers(2);
INSERT INTO select_final SELECT toDate('2020-01-01'), number + 1, '' FROM numbers(2);


SELECT * FROM select_final FINAL ORDER BY x;

TRUNCATE TABLE select_final;

INSERT INTO select_final SELECT toDate('2000-01-01'), number, '' FROM numbers(2);
INSERT INTO select_final SELECT toDate('2000-01-01'), number, 'updated' FROM numbers(2);

OPTIMIZE TABLE select_final FINAL;

INSERT INTO select_final SELECT toDate('2020-01-01'), number, '' FROM numbers(2);
INSERT INTO select_final SELECT toDate('2020-01-01'), number, 'updated' FROM numbers(2);

SELECT max(x) FROM select_final FINAL where string = 'updated';

TRUNCATE TABLE select_final;

INSERT INTO select_final SELECT toDate('2000-01-01'), number, '' FROM numbers(500000);

OPTIMIZE TABLE select_final FINAL;

SET remote_filesystem_read_method = 'read';

SELECT max(x) FROM select_final FINAL;

SYSTEM FLUSH LOGS;

SELECT length(thread_ids) FROM system.query_log WHERE query='SELECT max(x) FROM select_final FINAL;' AND type='QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1;

DROP TABLE select_final;
