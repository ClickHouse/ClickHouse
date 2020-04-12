DROP TABLE IF EXISTS m3;
DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS x;

CREATE TABLE x (d Date, t DateTime) ENGINE = MergeTree(d, (d, t), 1);

CREATE MATERIALIZED VIEW m1 (d Date, t DateTime, c UInt64) ENGINE = SummingMergeTree(d, (d, t), 1) AS SELECT d, toStartOfMinute(x.t) as t, count() as c FROM x GROUP BY d, t;

CREATE MATERIALIZED VIEW m3 ENGINE = SummingMergeTree(d, (d, t), 1) AS SELECT d, toStartOfHour(m1.t) as t, c FROM m1;

INSERT INTO x VALUES (today(), now());
INSERT INTO x VALUES (today(), now());

OPTIMIZE TABLE m3;

DROP TABLE m3;
DROP TABLE m1;
DROP TABLE x;
