DROP TABLE IF EXISTS set;
CREATE TABLE set (x String) ENGINE = Memory;
INSERT INTO set VALUES ('hello');
SELECT (arrayJoin(['hello', 'world']) AS s) IN set, s;

DROP TABLE set;
CREATE TABLE set (x String) ENGINE = Set;
INSERT INTO set VALUES ('hello');
SELECT (arrayJoin(['hello', 'world']) AS s) IN set, s;

DROP TABLE set;

DROP TABLE IF EXISTS join;
CREATE TABLE join (k UInt8, x String) ENGINE = Memory;
INSERT INTO join VALUES (1, 'hello');
SELECT k, x FROM (SELECT arrayJoin([1, 2]) AS k) ANY LEFT JOIN join USING k;

DROP TABLE join;
CREATE TABLE join (k UInt8, x String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join VALUES (1, 'hello');
SELECT k, x FROM (SELECT arrayJoin([1, 2]) AS k) ANY LEFT JOIN join USING k;

DROP TABLE join;
