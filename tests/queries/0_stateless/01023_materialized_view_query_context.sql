-- Create dictionary, since dictGet*() uses DB::Context in executeImpl()
-- (To cover scope of the Context in DB::PushingToViewsBlockOutputStream::process)

set insert_distributed_sync=1;

DROP TABLE IF EXISTS mv;
DROP DATABASE IF EXISTS dict_in_01023;
CREATE DATABASE dict_in_01023;

CREATE TABLE dict_in_01023.input (key UInt64, val UInt64) Engine=Memory();

CREATE DICTIONARY dict_in_01023.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 1
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'input' PASSWORD '' DB 'dict_in_01023'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

CREATE TABLE input    (key UInt64) Engine=Distributed(test_shard_localhost, currentDatabase(), buffer_, key);
CREATE TABLE null_    (key UInt64) Engine=Null();
CREATE TABLE buffer_  (key UInt64) Engine=Buffer(currentDatabase(), dist_out, 1, 0, 0, 0, 0, 0, 0);
CREATE TABLE dist_out (key UInt64) Engine=Distributed(test_shard_localhost, currentDatabase(), null_, key);

CREATE TABLE output (key UInt64, val UInt64) Engine=Memory();
CREATE MATERIALIZED VIEW mv TO output AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) val FROM dist_out;

INSERT INTO input VALUES (1);

SELECT count() FROM output;

DROP TABLE mv;
DROP TABLE output;
DROP TABLE dist_out;
DROP TABLE buffer_;
DROP TABLE null_;
DROP TABLE input;
DROP DICTIONARY dict_in_01023.dict;
DROP TABLE dict_in_01023.input;
DROP DATABASE dict_in_01023;
