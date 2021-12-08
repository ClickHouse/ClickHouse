-- Create table under database with engine ordinary.
set mutations_sync = 1;
set replication_alter_partitions_sync = 2;
DROP DATABASE IF EXISTS test_meta_cache on cluster preonline_hk5;
DROP TABLE IF EXISTS test_meta_cache.check_part_meta_cache on cluster preonline_hk5;
CREATE DATABASE test_meta_cache on cluster preonline_hk5 ENGINE = Atomic;
CREATE TABLE test_meta_cache.check_part_meta_cache on cluster preonline_hk5 ( p Date, k UInt64, v1 UInt64, v2 Int64) ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/test_meta_cache/check_part_meta_cache', '{replica}') PARTITION BY toYYYYMM(p) ORDER BY k;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Insert first batch of data.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Insert second batch of data.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Update some data.
alter table test_meta_cache.check_part_meta_cache update  v1 = 2001  where k = 1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

alter table test_meta_cache.check_part_meta_cache update  v2 = 4002  where k = 1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

--Delete some data.
alter table test_meta_cache.check_part_meta_cache delete where k = 1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

alter table test_meta_cache.check_part_meta_cache delete where k = 8;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Delete some data.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Drop partitioin 201805
alter table test_meta_cache.check_part_meta_cache drop partition 201805;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Optimize table.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
optimize table test_meta_cache.check_part_meta_cache FINAL;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Add column.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 add column v3 UInt64;
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Delete column.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 drop column v1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Add TTL info.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 modify TTL p + INTERVAL 10 YEAR;
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v2, v3) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Modify TTL info.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 modify TTL p + INTERVAL 15 YEAR;
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v2, v3) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Truncate table.
truncate table test_meta_cache.check_part_meta_cache;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Recreate table with projection.
drop table if exists test_meta_cache.check_part_meta_cache on cluster preonline_hk5;
CREATE TABLE test_meta_cache.check_part_meta_cache on cluster preonline_hk5 ( p Date, k UInt64, v1 UInt64, v2 Int64, projection p1 (select p, sum(k), sum(v1), sum(v2) group by p)) ENGINE MergeTree() PARTITION BY toYYYYMM(p) ORDER BY k;

-- Insert first batch of data.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Insert second batch of data.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-05-15', 5, 1000, 2000), ('2018-05-16', 6, 3000, 4000), ('2018-05-17', 7, 5000, 6000), ('2018-05-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Update some data.
alter table test_meta_cache.check_part_meta_cache update  v1 = 2001  where k = 1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

alter table test_meta_cache.check_part_meta_cache update  v2 = 4002  where k = 1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

--Delete some data.
alter table test_meta_cache.check_part_meta_cache delete where k = 1;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

alter table test_meta_cache.check_part_meta_cache delete where k = 8;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Delete some data.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Drop partitioin 201805
alter table test_meta_cache.check_part_meta_cache drop partition 201805;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Optimize table.
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
optimize table test_meta_cache.check_part_meta_cache FINAL;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Add column.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 add column v3 UInt64;
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Add TTL info.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 modify TTL p + INTERVAL 10 YEAR;
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Modify TTL info.
alter table test_meta_cache.check_part_meta_cache on cluster preonline_hk5 modify TTL p + INTERVAL 15 YEAR;
INSERT INTO test_meta_cache.check_part_meta_cache (p, k, v1, v2) VALUES ('2018-06-15', 5, 1000, 2000), ('2018-06-16', 6, 3000, 4000), ('2018-06-17', 7, 5000, 6000), ('2018-06-18', 8, 7000, 8000);
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

-- Truncate table.
truncate table test_meta_cache.check_part_meta_cache;
with arrayJoin(checkPartMetaCache('test_meta_cache', 'check_part_meta_cache')) as info select count(1), countIf(info.5 = 0);

