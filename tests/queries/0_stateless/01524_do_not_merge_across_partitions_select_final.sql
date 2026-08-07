-- Tags: no-fasttest

DROP TABLE IF EXISTS select_final;

SET allow_asynchronous_read_from_io_pool_for_merge_tree = 0;
SET do_not_merge_across_partitions_select_final = 1;
SET max_threads = 16;

CREATE TABLE select_final (t DateTime, x Int32, string String) ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(t) ORDER BY (x, t) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO select_final SELECT toDate('2000-01-01'), number, '' FROM numbers(2);
INSERT INTO select_final SELECT toDate('2000-01-01'), number + 1, '' FROM numbers(2);

INSERT INTO select_final SELECT toDate('2020-01-01'), number, '' FROM numbers(2);
INSERT INTO select_final SELECT toDate('2020-01-01'), number + 1, '' FROM numbers(2);

SELECT * FROM select_final FINAL ORDER BY x, t;

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
SELECT max(x) FROM select_final FINAL;

DROP TABLE select_final;
