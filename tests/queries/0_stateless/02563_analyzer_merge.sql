-- Tags: no-parallel

SET enable_analyzer = 1;

DROP DATABASE IF EXISTS 02563_db;
CREATE DATABASE 02563_db;

DROP TABLE IF EXISTS 02563_db.test_merge_table_1;
CREATE TABLE 02563_db.test_merge_table_1
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO 02563_db.test_merge_table_1 VALUES (0, 'Value_0');

DROP TABLE IF EXISTS 02563_db.test_merge_table_2;
CREATE TABLE 02563_db.test_merge_table_2
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO 02563_db.test_merge_table_2 VALUES (1, 'Value_1');

DROP TABLE IF EXISTS 02563_db.test_merge_table;
CREATE TABLE 02563_db.test_merge_table
(
    id UInt64,
    value String
) ENGINE=Merge(02563_db, '^test_merge_table');

SELECT id, value, _database, _table FROM 02563_db.test_merge_table ORDER BY id;

DROP TABLE 02563_db.test_merge_table;
DROP TABLE 02563_db.test_merge_table_1;
DROP TABLE 02563_db.test_merge_table_2;

CREATE TABLE 02563_db.t_1
(
    timestamp DateTime64(9),
    a String,
    b String
)
ENGINE = MergeTree
PARTITION BY formatDateTime(toStartOfMinute(timestamp), '%Y%m%d%H', 'UTC')
ORDER BY (timestamp, a, b);

CREATE TABLE 02563_db.dist_t_1 (timestamp DateTime64(9), a String, b String) ENGINE = Distributed('test_shard_localhost', '02563_db', 't_1');

CREATE TABLE 02563_db.m ENGINE = Merge('02563_db', '^dist_');

INSERT INTO 02563_db.t_1 (timestamp, a, b)
select
    addMinutes(toDateTime64('2024-07-13 22:00:00', 9, 'UTC'), number),
    randomString(5),
    randomString(5)
from numbers(30);

INSERT INTO 02563_db.t_1 (timestamp, a, b)
select
    addMinutes(toDateTime64('2024-07-13 23:00:00', 9, 'UTC'), number),
    randomString(5),
    randomString(5)
from numbers(30);

INSERT INTO 02563_db.t_1 (timestamp, a, b)
select
    addMinutes(toDateTime64('2024-07-14 00:00:00', 9, 'UTC'), number),
    randomString(5),
    randomString(5)
from numbers(100);


SELECT '91138316-5127-45ac-9c25-4ad8779777b4',
  count()
FROM 02563_db.m;

DROP TABLE 02563_db.t_1;
DROP TABLE 02563_db.dist_t_1;
DROP TABLE 02563_db.m;

DROP DATABASE 02563_db;
