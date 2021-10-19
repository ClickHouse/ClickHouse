-- Tags: zookeeper

DROP TABLE IF EXISTS broken_partition;

CREATE TABLE broken_partition
(
    date Date,
    key UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/test_01925_{database}/rmt', 'r1')
ORDER BY tuple()
PARTITION BY date;

ALTER TABLE broken_partition DROP PARTITION ID '20210325_0_13241_6_12747'; --{serverError 248}

ALTER TABLE broken_partition DROP PARTITION ID '20210325_0_13241_6_12747'; --{serverError 248}

DROP TABLE IF EXISTS broken_partition;

DROP TABLE IF EXISTS old_partition_key;

CREATE TABLE old_partition_key (sd Date, dh UInt64, ak UInt32, ed Date) ENGINE=MergeTree(sd, dh, (ak, ed, dh), 8192);

ALTER TABLE old_partition_key DROP PARTITION ID '20210325_0_13241_6_12747'; --{serverError 248}

ALTER TABLE old_partition_key DROP PARTITION ID '202103';

DROP TABLE old_partition_key;
