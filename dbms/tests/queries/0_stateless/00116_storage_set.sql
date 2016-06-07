DROP TABLE IF EXISTS test.set;
DROP TABLE IF EXISTS test.set2;

CREATE TABLE test.set (x String) ENGINE = Set;

USE test;

SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s NOT IN set;

INSERT INTO set VALUES ('Hello'), ('World');
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

RENAME TABLE set TO set2;
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO test.set2 VALUES ('Hello'), ('World');
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO test.set2 VALUES ('abc'), ('World');
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

DETACH TABLE set2;
ATTACH TABLE set2 (x String) ENGINE = Set;

SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

RENAME TABLE set2 TO set;
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

USE default;

DROP TABLE test.set;
