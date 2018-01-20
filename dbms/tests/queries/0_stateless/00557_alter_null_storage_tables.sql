DROP TABLE IF EXISTS test.null;

CREATE TABLE test.null (x UInt8) ENGINE = Null;
DESCRIBE TABLE test.null;

ALTER TABLE test.null ADD COLUMN y String, MODIFY COLUMN x Int64 DEFAULT toInt64(y);
DESCRIBE TABLE test.null;

DROP TABLE test.null;
