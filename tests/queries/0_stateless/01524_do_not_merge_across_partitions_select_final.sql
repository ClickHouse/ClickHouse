DROP TABLE IF EXISTS select_final;

CREATE TABLE select_final (t DateTime, x Int32) ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(t) ORDER BY x; 

INSERT INTO select_final SELECT toDate('2000-01-01'), number FROM numbers(2);
INSERT INTO select_final SELECT toDate('2000-01-01'), number + 1 FROM numbers(2);

INSERT INTO select_final SELECT toDate('2020-01-01'), number FROM numbers(2);
INSERT INTO select_final SELECT toDate('2020-01-01'), number + 1 FROM numbers(2);


SELECT * FROM select_final FINAL ORDER BY x SETTINGS do_not_merge_across_partitions_select_final = 1;

DROP TABLE select_final;

