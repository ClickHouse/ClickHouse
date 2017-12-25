DROP TABLE IF EXISTS test.m3;
DROP TABLE IF EXISTS test.m1;
DROP TABLE IF EXISTS test.x;

CREATE TABLE test.x (d Date, t DateTime) ENGINE = MergeTree(d, (d, t), 1);

CREATE MATERIALIZED VIEW test.m1 (d Date, t DateTime, c UInt64) ENGINE = SummingMergeTree(d, (d, t), 1) AS SELECT d, toStartOfMinute(x.t) as t, count() as c FROM test.x GROUP BY d, t;

CREATE MATERIALIZED VIEW test.m3 ENGINE = SummingMergeTree(d, (d, t), 1) AS SELECT d, toStartOfHour(m1.t) as t, c FROM test.m1;

INSERT INTO test.x VALUES (today(), now());
INSERT INTO test.x VALUES (today(), now());

OPTIMIZE TABLE test.m3;

DROP TABLE test.m3;
DROP TABLE test.m1;
DROP TABLE test.x;
