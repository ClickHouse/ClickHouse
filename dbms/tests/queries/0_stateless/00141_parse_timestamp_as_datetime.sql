DROP TABLE IF EXISTS test.default;

CREATE TABLE test.default (d Date DEFAULT toDate(t), t DateTime) ENGINE = MergeTree(d, t, 8192);
INSERT INTO test.default (t) VALUES ('1234567890');
SELECT toStartOfMonth(d), toUInt32(t) FROM test.default;

DROP TABLE test.default;
