
DROP TABLE IF EXISTS issue_repro;
DROP TABLE IF EXISTS issue_repro1;

CREATE TABLE issue_repro (c1 Date32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO issue_repro SELECT toDate32('1971-01-01') + toIntervalDay(number % 18000) FROM numbers(9991);
INSERT INTO issue_repro SELECT toDate32('1905-01-01') + toIntervalDay(number * 30)    FROM numbers(9);
OPTIMIZE TABLE issue_repro FINAL;
SELECT count() FROM issue_repro WHERE toStartOfYear(c1) < toStartOfYear(toDate('2021-06-15'));

CREATE TABLE issue_repro1 (c1 Date32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
insert into issue_repro1 select * from issue_repro;
SELECT count() FROM issue_repro1 WHERE toStartOfYear(c1) < toStartOfYear(toDate('2021-06-15'));

DROP TABLE issue_repro;
DROP TABLE issue_repro1;
