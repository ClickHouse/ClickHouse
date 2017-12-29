
SELECT X FROM (SELECT * FROM (SELECT 1 AS X, 2 AS Y) UNION ALL SELECT 3, 4) ORDER BY X;

DROP TABLE IF EXISTS test.globalin;

CREATE TABLE test.globalin (CounterID UInt32, StartDate Date ) ENGINE = Memory;

INSERT INTO test.globalin VALUES (34, toDate('2017-10-02')), (42, toDate('2017-10-02')), (55, toDate('2017-10-01'));

SELECT * FROM ( SELECT CounterID FROM remote('localhost', 'test', 'globalin') WHERE (CounterID GLOBAL IN ( SELECT toUInt32(34))) GROUP BY CounterID);
SELECT 'NOW okay =========================:';
SELECT CounterID FROM remote('localhost', 'test', 'globalin') WHERE (CounterID GLOBAL IN ( SELECT toUInt32(34) )) GROUP BY CounterID  UNION ALL SELECT CounterID FROM remote('localhost', 'test', 'globalin') WHERE (CounterID GLOBAL IN ( SELECT toUInt32(34))) GROUP BY CounterID;
SELECT 'NOW BAD ==========================:';
SELECT * FROM ( SELECT CounterID FROM remote('localhost', 'test', 'globalin') WHERE (CounterID GLOBAL IN ( SELECT toUInt32(34) )) GROUP BY CounterID  UNION ALL SELECT CounterID FROM remote('localhost', 'test', 'globalin') WHERE (CounterID GLOBAL IN ( SELECT toUInt32(34))) GROUP BY CounterID);
SELECT 'finish ===========================;';

DROP TABLE test.globalin;


DROP TABLE IF EXISTS test.union_bug;

CREATE TABLE test.union_bug (
    Event String,
    Datetime DateTime
) Engine = Memory;

INSERT INTO test.union_bug VALUES ('A', 1), ('B', 2);

SELECT ' * A UNION * B:';
SELECT * FROM (
  SELECT * FROM test.union_bug WHERE Event = 'A'
 UNION ALL
  SELECT * FROM test.union_bug WHERE Event = 'B'
) ORDER BY Datetime;

SELECT ' Event, Datetime A UNION * B:';
SELECT * FROM (
  SELECT Event, Datetime FROM test.union_bug WHERE Event = 'A'
 UNION ALL
  SELECT * FROM test.union_bug WHERE Event = 'B'
) ORDER BY Datetime;

SELECT ' * A UNION Event, Datetime B:';
SELECT * FROM (
  SELECT * FROM test.union_bug WHERE Event = 'A'
 UNION ALL
  SELECT Event, Datetime FROM test.union_bug WHERE Event = 'B'
) ORDER BY Datetime;

SELECT ' Event, Datetime A UNION Event, Datetime B:';
SELECT * FROM (
  SELECT Event, Datetime FROM test.union_bug WHERE Event = 'A'
 UNION ALL
  SELECT Event, Datetime FROM test.union_bug WHERE Event = 'B'
) ORDER BY Datetime;


DROP TABLE test.union_bug;
