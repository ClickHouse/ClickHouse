SET send_logs_level = 'none';
SET allow_experimental_data_skipping_indices = 1;

DROP TABLE IF EXISTS mt_with_pk;

CREATE TABLE mt_with_pk (
  d Date DEFAULT '2000-01-01',
  x DateTime,
  y Array(UInt64),
  z UInt64,
  n Nested (Age UInt8, Name String),
  w Int16 DEFAULT 10
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d) ORDER BY (x, z) SETTINGS index_granularity_bytes=10000;

SELECT '===test insert===';

INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (toDate('2018-10-01'), toDateTime('2018-10-01 12:57:57'), [1, 1, 1], 11, [77], ['Joe']), (toDate('2018-10-01'), toDateTime('2018-10-01 16:57:57'), [2, 2, 2], 12, [88], ['Mark']), (toDate('2018-10-01'), toDateTime('2018-10-01 19:57:57'), [3, 3, 3], 13, [99], ['Robert']);

SELECT COUNT(*) FROM mt_with_pk WHERE x > toDateTime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND active=1;

SELECT '===test merge===';
INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (toDate('2018-10-01'), toDateTime('2018-10-01 07:57:57'), [4, 4, 4], 14, [111, 222], ['Lui', 'Dave']), (toDate('2018-10-01'), toDateTime('2018-10-01 08:57:57'), [5, 5, 5], 15, [333, 444], ['John', 'Mike']), (toDate('2018-10-01'), toDateTime('2018-10-01 09:57:57'), [6, 6, 6], 16, [555, 666, 777], ['Alex', 'Jim', 'Tom']);

OPTIMIZE TABLE mt_with_pk FINAL;

SELECT COUNT(*) FROM mt_with_pk WHERE x > toDateTime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND active=1;

SELECT '===test alter===';
ALTER TABLE mt_with_pk MODIFY COLUMN y Array(String);

INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (toDate('2018-10-01'), toDateTime('2018-10-01 05:57:57'), ['a', 'a', 'a'], 14, [888, 999], ['Jack', 'Elvis']);

OPTIMIZE TABLE mt_with_pk FINAL;

SELECT COUNT(*) FROM mt_with_pk WHERE x > toDateTime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND active=1;

SELECT '===test mutation===';
ALTER TABLE mt_with_pk UPDATE w = 0 WHERE 1;
ALTER TABLE mt_with_pk UPDATE y = ['q', 'q', 'q'] WHERE 1;

SELECT sleep(3) format Null;

SELECT sum(w) FROM mt_with_pk;
SELECT distinct(y) FROM mt_with_pk;

OPTIMIZE TABLE mt_with_pk FINAL;

SELECT '===test skip_idx===';

ALTER TABLE mt_with_pk ADD INDEX idx1 z + w TYPE minmax GRANULARITY 1;

INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`, w) VALUES (toDate('2018-10-01'), toDateTime('2018-10-01 03:57:57'), ['z', 'z', 'z'], 15, [1111, 2222], ['Garry', 'Ron'], 1);

OPTIMIZE TABLE mt_with_pk FINAL;

SELECT COUNT(*) FROM mt_with_pk WHERE z + w > 5000;

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND active=1;

--DROP TABLE IF EXISTS mt_with_pk;
