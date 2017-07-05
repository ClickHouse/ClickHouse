DROP TABLE IF EXISTS test.insert;
CREATE TABLE test.insert (i UInt64, s String, u UUID, d Date, t DateTime, a Array(UInt32)) ENGINE = Memory;

INSERT INTO test.insert VALUES (1, 'Hello', 'ab41bdd6-5cd4-11e7-907b-a6006ad3dba0', '2016-01-01', '2016-01-02 03:04:05', [1, 2, 3]), (1 + 1, concat('Hello', ', world'), toUUID(0), toDate('2016-01-01') + 1, toStartOfMinute(toDateTime('2016-01-02 03:04:05')), [[0,1],[2]][1]), (round(pi()), concat('hello', ', world!'), toUUID(toString('ab41bdd6-5cd4-11e7-907b-a6006ad3dba0')), toDate(toDateTime('2016-01-03 03:04:05')), toStartOfHour(toDateTime('2016-01-02 03:04:05')), []), (4, 'World', 'ab41bdd6-5cd4-11e7-907b-a6006ad3dba0', '2016-01-04', '2016-12-11 10:09:08', [3,2,1]);

SELECT * FROM test.insert ORDER BY i;
DROP TABLE test.insert;
