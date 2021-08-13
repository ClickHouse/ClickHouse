SET send_logs_level = 'fatal';
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS alter_compression_codec1;
DROP TABLE IF EXISTS alter_compression_codec2;

CREATE TABLE alter_compression_codec1 (
    somedate Date CODEC(LZ4),
    id UInt64 CODEC(NONE)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00910/alter_compression_codecs', '1') PARTITION BY somedate ORDER BY id;

CREATE TABLE alter_compression_codec2 (
  somedate Date CODEC(LZ4),
  id UInt64 CODEC(NONE)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00910/alter_compression_codecs', '2') PARTITION BY somedate ORDER BY id;

INSERT INTO alter_compression_codec1 VALUES('2018-01-01', 1);
INSERT INTO alter_compression_codec1 VALUES('2018-01-01', 2);
SYSTEM SYNC REPLICA alter_compression_codec2;

SELECT * FROM alter_compression_codec1 ORDER BY id;
SELECT * FROM alter_compression_codec2 ORDER BY id;

ALTER TABLE alter_compression_codec1 ADD COLUMN alter_column String DEFAULT 'default_value' CODEC(ZSTD);
SYSTEM SYNC REPLICA alter_compression_codec1;
SYSTEM SYNC REPLICA alter_compression_codec2;

SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec2' AND name = 'alter_column';

INSERT INTO alter_compression_codec1 VALUES('2018-01-01', 3, '3');
INSERT INTO alter_compression_codec1 VALUES('2018-01-01', 4, '4');
SYSTEM SYNC REPLICA alter_compression_codec1;
SYSTEM SYNC REPLICA alter_compression_codec2;

SELECT * FROM alter_compression_codec1 ORDER BY id;
SELECT * FROM alter_compression_codec2 ORDER BY id;

ALTER TABLE alter_compression_codec1 MODIFY COLUMN alter_column CODEC(NONE);
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec2' AND name = 'alter_column';

INSERT INTO alter_compression_codec2 VALUES('2018-01-01', 5, '5');
INSERT INTO alter_compression_codec2 VALUES('2018-01-01', 6, '6');
SYSTEM SYNC REPLICA alter_compression_codec1;
SELECT * FROM alter_compression_codec1 ORDER BY id;
SELECT * FROM alter_compression_codec2 ORDER BY id;

SET allow_suspicious_codecs = 1;
ALTER TABLE alter_compression_codec1 MODIFY COLUMN alter_column CODEC(ZSTD, LZ4HC, LZ4, LZ4, NONE);
SYSTEM SYNC REPLICA alter_compression_codec1;
SYSTEM SYNC REPLICA alter_compression_codec2;
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec2' AND name = 'alter_column';

INSERT INTO alter_compression_codec1 VALUES('2018-01-01', 7, '7');
INSERT INTO alter_compression_codec2 VALUES('2018-01-01', 8, '8');
SYSTEM SYNC REPLICA alter_compression_codec2;
SYSTEM SYNC REPLICA alter_compression_codec1;
SELECT * FROM alter_compression_codec1 ORDER BY id;
SELECT * FROM alter_compression_codec2 ORDER BY id;

ALTER TABLE alter_compression_codec1 MODIFY COLUMN alter_column FixedString(100);
SYSTEM SYNC REPLICA alter_compression_codec2;
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec1' AND name = 'alter_column';
SELECT compression_codec FROM system.columns WHERE table = 'alter_compression_codec2' AND name = 'alter_column';

DROP TABLE IF EXISTS alter_compression_codec1;
DROP TABLE IF EXISTS alter_compression_codec2;
