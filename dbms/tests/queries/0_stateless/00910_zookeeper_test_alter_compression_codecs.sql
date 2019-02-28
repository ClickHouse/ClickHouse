SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.alter_compression_codec1;
DROP TABLE IF EXISTS test.alter_compression_codec2;

CREATE TABLE test.alter_compression_codec1 (
    somedate Date CODEC(LZ4),
    id UInt64 CODEC(NONE)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter_compression_codecs', '1') PARTITION BY somedate ORDER BY id;

CREATE TABLE test.alter_compression_codec2 (
  somedate Date CODEC(LZ4),
  id UInt64 CODEC(NONE)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/alter_compression_codecs', '2') PARTITION BY somedate ORDER BY id;

INSERT INTO test.alter_compression_codec1 VALUES('2018-01-01', 1);
INSERT INTO test.alter_compression_codec1 VALUES('2018-01-01', 2);
SYSTEM SYNC REPLICA test.alter_compression_codec2;

SELECT * FROM test.alter_compression_codec1 ORDER BY id;
SELECT * FROM test.alter_compression_codec2 ORDER BY id;

ALTER TABLE test.alter_compression_codec1 ADD COLUMN alter_column String DEFAULT 'default_value' CODEC(ZSTD);
SYSTEM SYNC REPLICA test.alter_compression_codec2;

SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec2' AND name = 'alter_column';

INSERT INTO test.alter_compression_codec1 VALUES('2018-01-01', 3, '3');
INSERT INTO test.alter_compression_codec1 VALUES('2018-01-01', 4, '4');
SYSTEM SYNC REPLICA test.alter_compression_codec2;

SELECT * FROM test.alter_compression_codec1 ORDER BY id;
SELECT * FROM test.alter_compression_codec2 ORDER BY id;

ALTER TABLE test.alter_compression_codec1 MODIFY COLUMN alter_column CODEC(NONE);
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec2' AND name = 'alter_column';

INSERT INTO test.alter_compression_codec2 VALUES('2018-01-01', 5, '5');
INSERT INTO test.alter_compression_codec2 VALUES('2018-01-01', 6, '6');
SYSTEM SYNC REPLICA test.alter_compression_codec1;
SELECT * FROM test.alter_compression_codec1 ORDER BY id;
SELECT * FROM test.alter_compression_codec2 ORDER BY id;

ALTER TABLE test.alter_compression_codec1 MODIFY COLUMN alter_column CODEC(ZSTD, LZ4HC, LZ4, LZ4, NONE);
SYSTEM SYNC REPLICA test.alter_compression_codec1;
SYSTEM SYNC REPLICA test.alter_compression_codec2;
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec2' AND name = 'alter_column';

INSERT INTO test.alter_compression_codec1 VALUES('2018-01-01', 7, '7');
INSERT INTO test.alter_compression_codec2 VALUES('2018-01-01', 8, '8');
SYSTEM SYNC REPLICA test.alter_compression_codec2;
SYSTEM SYNC REPLICA test.alter_compression_codec1;
SELECT * FROM test.alter_compression_codec1 ORDER BY id;
SELECT * FROM test.alter_compression_codec2 ORDER BY id;

ALTER TABLE test.alter_compression_codec1 MODIFY COLUMN alter_column FixedString(100);
SYSTEM SYNC REPLICA test.alter_compression_codec2;
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE database = 'test' AND table = 'alter_compression_codec2' AND name = 'alter_column';

DROP TABLE IF EXISTS test.alter_compression_codec1;
DROP TABLE IF EXISTS test.alter_compression_codec2;
