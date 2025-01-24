CREATE TABLE datetime64_issue (id int, dt DateTime64(3), dtn Nullable(DateTime64(3))) ENGINE = MergeTree() ORDER BY id PRIMARY KEY id;

INSERT INTO datetime64_issue(id, dt, dtn) VALUES (1, toDateTime64('2001-01-11 01:11:21.100', 3), toDateTime64('2001-01-11 01:11:21.100', 3));

SELECT * FROM datetime64_issue WHERE dt in (toDateTime64('2001-01-11 01:11:21.100', 3));

SELECT * FROM datetime64_issue WHERE dtn in (toDateTime64('2001-01-11 01:11:21.100', 3));

DROP TABLE datetime64_issue;
