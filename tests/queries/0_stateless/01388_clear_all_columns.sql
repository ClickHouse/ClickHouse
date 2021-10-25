DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
ALTER TABLE test CLEAR COLUMN x; --{serverError 36}
DROP TABLE test;

DROP TABLE IF EXISTS test;

CREATE TABLE test (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x, y) VALUES (1, 1), (2, 2), (3, 3);

ALTER TABLE test CLEAR COLUMN x;

ALTER TABLE test CLEAR COLUMN x IN PARTITION ''; --{serverError 248}
ALTER TABLE test CLEAR COLUMN x IN PARTITION 'asdasd'; --{serverError 248}
ALTER TABLE test CLEAR COLUMN x IN PARTITION '123'; --{serverError 248}

ALTER TABLE test CLEAR COLUMN y; --{serverError 36}

ALTER TABLE test ADD COLUMN z String DEFAULT 'Hello';

-- y is only real column in table
ALTER TABLE test CLEAR COLUMN y; --{serverError 36}
ALTER TABLE test CLEAR COLUMN x;
ALTER TABLE test CLEAR COLUMN z;

INSERT INTO test (x, y, z) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

ALTER TABLE test CLEAR COLUMN z;
ALTER TABLE test CLEAR COLUMN x;

SELECT * FROM test ORDER BY y;

DROP TABLE IF EXISTS test;
