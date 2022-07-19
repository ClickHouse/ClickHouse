-- Tags: no-parallel

DROP TABLE IF EXISTS set;
DROP TABLE IF EXISTS set2;

CREATE TABLE set (x String) ENGINE = Set;

SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s NOT IN set;

INSERT INTO set VALUES ('Hello'), ('World');
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

RENAME TABLE set TO set2;
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO set2 VALUES ('Hello'), ('World');
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO set2 VALUES ('abc'), ('World');
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

DETACH TABLE set2;
ATTACH TABLE set2;

SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

RENAME TABLE set2 TO set;
SELECT arrayJoin(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

DROP TABLE set;
