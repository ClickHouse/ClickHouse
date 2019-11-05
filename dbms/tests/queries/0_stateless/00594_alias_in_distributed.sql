DROP TABLE IF EXISTS alias_local10;
DROP TABLE IF EXISTS alias10;

CREATE TABLE alias_local10 (
  Id Int8,
  EventDate Date DEFAULT '2000-01-01',
  field1 Int8,
  field2 String,
  field3 ALIAS CASE WHEN field1 = 1 THEN field2 ELSE '0' END
) ENGINE = MergeTree(EventDate, (Id, EventDate), 8192);

CREATE TABLE alias10 AS alias_local10 ENGINE = Distributed(test_shard_localhost, currentDatabase(), alias_local10, cityHash64(Id));

INSERT INTO alias_local10 (Id, EventDate, field1, field2) VALUES (1, '2000-01-01', 1, '12345'), (2, '2000-01-01', 2, '54321'), (3, '2000-01-01', 0, '');

SELECT field1, field2, field3 FROM alias_local10;
SELECT field1, field2, field3 FROM alias_local10 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias_local10 WHERE EventDate='2000-01-01';

SELECT field1, field2, field3 FROM alias10;
SELECT field1, field2, field3 FROM alias10 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias10 WHERE EventDate='2000-01-01';

SELECT field2, field3 FROM alias10 WHERE EventDate='2000-01-01';
SELECT field3 FROM alias10 WHERE EventDate='2000-01-01';
SELECT field2, field3 FROM alias10;
SELECT field3 FROM alias10;

SELECT field1 FROM alias10 WHERE field3 = '12345';
SELECT field2 FROM alias10 WHERE field3 = '12345';
SELECT field3 FROM alias10 WHERE field3 = '12345';

DROP TABLE alias10;
CREATE TABLE alias10 (
  Id Int8,
  EventDate Date,
  field1 Int8,
  field2 String,
  field3 String
) ENGINE = Distributed(test_shard_localhost, currentDatabase(), alias_local10);

SELECT field1, field2, field3 FROM alias_local10;
SELECT field1, field2, field3 FROM alias_local10 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias_local10 WHERE EventDate='2000-01-01';

SELECT field1, field2, field3 FROM alias10;
SELECT field1, field2, field3 FROM alias10 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias10 WHERE EventDate='2000-01-01';

SELECT field2, field3 FROM alias10 WHERE EventDate='2000-01-01';
SELECT field3 FROM alias10 WHERE EventDate='2000-01-01';
SELECT field2, field3 FROM alias10;
SELECT field3 FROM alias10;

SELECT field1 FROM alias10 WHERE field3 = '12345';
SELECT field2 FROM alias10 WHERE field3 = '12345';
SELECT field3 FROM alias10 WHERE field3 = '12345';

DROP TABLE alias_local10;
DROP TABLE alias10;
