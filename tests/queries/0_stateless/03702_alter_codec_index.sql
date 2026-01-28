DROP TABLE IF EXISTS test_alter_codec_index;
CREATE TABLE test_alter_codec_index (`id` UInt64, value UInt64, INDEX id_index id TYPE minmax GRANULARITY 1) Engine=MergeTree() ORDER BY tuple();
INSERT INTO test_alter_codec_index SELECT number, number * number from numbers(100);
ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt64 CODEC(NONE);
ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt64 CODEC(Delta, LZ4);

ALTER TABLE test_alter_codec_index MODIFY SETTING alter_column_secondary_index_mode = 'throw';
ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt32 CODEC(Delta, LZ4); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- With rebuild the index will be updated, so ALTERs are allowed
ALTER TABLE test_alter_codec_index MODIFY SETTING alter_column_secondary_index_mode = 'rebuild';
ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt32 CODEC(Delta, LZ4);
SELECT sum(id) FROM test_alter_codec_index;

ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt64 DEFAULT 3 CODEC(Delta, LZ4);
SELECT sum(id) FROM test_alter_codec_index;

INSERT INTO test_alter_codec_index (value) VALUES (1);
SELECT sum(id) FROM test_alter_codec_index;

ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt64 ALIAS 3 CODEC(Delta, LZ4); -- { serverError INCORRECT_QUERY }

ALTER TABLE test_alter_codec_index MODIFY COLUMN id UInt64 MATERIALIZED 3 CODEC(Delta, LZ4);
SELECT sum(id) FROM test_alter_codec_index;

INSERT INTO test_alter_codec_index (value) VALUES (1);
SELECT sum(id) FROM test_alter_codec_index;

ALTER TABLE test_alter_codec_index MODIFY COLUMN id Int64;
SELECT sum(id) FROM test_alter_codec_index;

DROP TABLE IF EXISTS test_alter_codec_index;