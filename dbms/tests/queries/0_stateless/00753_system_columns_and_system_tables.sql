CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.check_system_tables;

-- Check MergeTree declaration in new format
CREATE TABLE test.check_system_tables
  (
    name1 UInt8,
    name2 UInt8,
    name3 UInt8
  ) ENGINE = MergeTree()
    ORDER BY name1
    PARTITION BY name2
    SAMPLE BY name1;

SELECT name, primary_key, order_key, partition_key, sample_key
FROM system.tables
WHERE name = 'check_system_tables'
FORMAT PrettyCompactNoEscapes;

SELECT name, is_in_primary_key, is_in_order_key, is_in_partition_key, is_in_sample_key
FROM system.columns
WHERE table = 'check_system_tables'
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF EXISTS test.check_system_tables;

-- Check VersionedCollapsingMergeTree
CREATE TABLE test.check_system_tables
  (
    date Date,
    value String,
    version UInt64,
    sign Int8
  ) ENGINE = VersionedCollapsingMergeTree(sign, version)
    PARTITION BY date
    ORDER BY date;

SELECT name, primary_key, order_key, partition_key, sample_key
FROM system.tables
WHERE name = 'check_system_tables'
FORMAT PrettyCompactNoEscapes;

SELECT name, is_in_primary_key, is_in_order_key, is_in_partition_key, is_in_sample_key
FROM system.columns
WHERE table = 'check_system_tables'
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF EXISTS test.check_system_tables;

-- Check MergeTree declaration in old format
CREATE TABLE test.check_system_tables
  (
    Event Date,
    UserId UInt32,
    Counter UInt32
  ) ENGINE = MergeTree(Event, intHash32(UserId), (Counter, Event, intHash32(UserId)), 8192);

SELECT name, primary_key, order_key, partition_key, sample_key
FROM system.tables
WHERE name = 'check_system_tables'
FORMAT PrettyCompactNoEscapes;

SELECT name, is_in_primary_key, is_in_order_key, is_in_partition_key, is_in_sample_key
FROM system.columns
WHERE table = 'check_system_tables'
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF EXISTS test.check_system_tables;
