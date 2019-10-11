DROP TABLE IF EXISTS nullable_alter;
CREATE TABLE nullable_alter (d Date DEFAULT '2000-01-01', x String) ENGINE = MergeTree(d, d, 1);

INSERT INTO nullable_alter (x) VALUES ('Hello'), ('World');
SELECT x FROM nullable_alter ORDER BY x;

ALTER TABLE nullable_alter MODIFY COLUMN x Nullable(String);
SELECT x FROM nullable_alter ORDER BY x;

INSERT INTO nullable_alter (x) VALUES ('xyz'), (NULL);
SELECT x FROM nullable_alter ORDER BY x NULLS FIRST;

ALTER TABLE nullable_alter MODIFY COLUMN x Nullable(FixedString(5));
SELECT x FROM nullable_alter ORDER BY x NULLS FIRST;

DROP TABLE nullable_alter;
