DROP TABLE IF EXISTS mv;
DROP DATABASE IF EXISTS dict_01521;
CREATE DATABASE dict_01521;

CREATE TABLE dict_01521.sharding_table (key UInt64, val UInt64) Engine=Memory();

CREATE DICTIONARY dict_01521.sharding_dict
(
  key UInt64 DEFAULT 0,
  val UInt8 DEFAULT 1
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'sharding_table' PASSWORD '' DB 'dict_01521'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

INSERT INTO dict_01521.sharding_table VALUES (150, 1), (151, 2);

CREATE TABLE table_first (a UInt64, b UInt64) ENGINE = Memory;
CREATE TABLE table_second (a UInt64, b UInt64) ENGINE = Memory;

CREATE TABLE table_distr (a Int) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), 't_local');



