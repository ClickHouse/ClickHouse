DROP TABLE IF EXISTS bug_13492;

CREATE TABLE bug_13492 (`d` DateTime) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(d) ORDER BY tuple();

INSERT INTO bug_13492 SELECT addDays(now(), number) FROM numbers(100);

SET max_threads = 5;

SELECT DISTINCT 1 FROM bug_13492, numbers(1) n;

SET max_threads = 2;

SELECT DISTINCT 1 FROM bug_13492, numbers(1) n;

DROP TABLE bug_13492;
