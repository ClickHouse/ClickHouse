CREATE TABLE alias1 (
  Id Int8,
  EventDate Date DEFAULT '2000-01-01',
  field1 Int8,
  field2 String,
  field3 ALIAS CASE WHEN field1 = 1 THEN field2 ELSE '0' END
) ENGINE = MergeTree(EventDate, (Id, EventDate), 8192);

CREATE TABLE alias2 AS alias1 ENGINE = Distributed(test_shard_localhost, default, alias1, cityHash64(Id));

INSERT INTO alias1 (Id, EventDate, field1, field2) VALUES (1, '2000-01-01', 1, '12345'), (2, '2000-01-01', 2, '54321'), (3, '2000-01-01', 0, '');

SELECT field1, field2, field3 FROM alias1;
SELECT field1, field2, field3 FROM alias1 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias1 WHERE EventDate='2000-01-01';

SELECT field1, field2, field3 FROM alias2;
SELECT field1, field2, field3 FROM alias2 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias2 WHERE EventDate='2000-01-01';

SELECT field2, field3 FROM alias2 WHERE EventDate='2000-01-01';
SELECT field3 FROM alias2 WHERE EventDate='2000-01-01';
SELECT field2, field3 FROM alias2;
SELECT field3 FROM alias2;

SELECT field1 FROM alias2 WHERE field3 = '12345';
SELECT field2 FROM alias2 WHERE field3 = '12345';
SELECT field3 FROM alias2 WHERE field3 = '12345';

CREATE TABLE alias3 (
  Id Int8,
  EventDate Date,
  field1 Int8,
  field2 String,
  field3 String
) ENGINE = Distributed(test_shard_localhost, default, alias1);

SELECT field1, field2, field3 FROM alias1;
SELECT field1, field2, field3 FROM alias1 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias1 WHERE EventDate='2000-01-01';

SELECT field1, field2, field3 FROM alias3;
SELECT field1, field2, field3 FROM alias3 WHERE EventDate='2000-01-01';
SELECT field1, field2 FROM alias3 WHERE EventDate='2000-01-01';

SELECT field2, field3 FROM alias3 WHERE EventDate='2000-01-01';
SELECT field3 FROM alias3 WHERE EventDate='2000-01-01';
SELECT field2, field3 FROM alias3;
SELECT field3 FROM alias3;

SELECT field1 FROM alias3 WHERE field3 = '12345';
SELECT field2 FROM alias3 WHERE field3 = '12345';
SELECT field3 FROM alias3 WHERE field3 = '12345';
