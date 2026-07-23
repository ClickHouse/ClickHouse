DROP TABLE IF EXISTS test_alter_codec_pk;
CREATE TABLE test_alter_codec_pk (id UInt64, value UInt64) Engine=MergeTree() ORDER BY id;
INSERT INTO test_alter_codec_pk SELECT number, number * number from numbers(100);
-- { echoOn }
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt64 CODEC(NONE);
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt64 CODEC(Delta, LZ4);
SELECT sum(id) FROM test_alter_codec_pk;
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt32 CODEC(Delta, LZ4); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt64 DEFAULT 3 CODEC(Delta, LZ4);
INSERT INTO test_alter_codec_pk (value) VALUES (1);
SELECT sum(id) FROM test_alter_codec_pk;
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt64 ALIAS 3 CODEC(Delta, LZ4); -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt64 MATERIALIZED 3 CODEC(Delta, LZ4);
INSERT INTO test_alter_codec_pk (value) VALUES (1);
SELECT sum(id) FROM test_alter_codec_pk;
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id UInt64;
ALTER TABLE test_alter_codec_pk MODIFY COLUMN id Int64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE IF EXISTS test_alter_codec_pk;