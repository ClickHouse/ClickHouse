DROP TABLE IF EXISTS test.set;
CREATE TABLE test.set (x String) ENGINE = Memory;
INSERT INTO test.set VALUES ('hello');
SELECT (arrayJoin(['hello', 'world']) AS s) IN test.set, s;

DROP TABLE test.set;
CREATE TABLE test.set (x String) ENGINE = Set;
INSERT INTO test.set VALUES ('hello');
SELECT (arrayJoin(['hello', 'world']) AS s) IN test.set, s;

DROP TABLE test.set;

DROP TABLE IF EXISTS test.join;
CREATE TABLE test.join (k UInt8, x String) ENGINE = Memory;
INSERT INTO test.join VALUES (1, 'hello');
SELECT k, x FROM (SELECT arrayJoin([1, 2]) AS k) ANY LEFT JOIN test.join USING k;

DROP TABLE test.join;
CREATE TABLE test.join (k UInt8, x String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO test.join VALUES (1, 'hello');
SELECT k, x FROM (SELECT arrayJoin([1, 2]) AS k) ANY LEFT JOIN test.join USING k;

DROP TABLE test.join;
