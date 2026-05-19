-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: Depends on AWS/MinIO paimon_all_types dataset
-- Tag no-parallel: Uses SYSTEM DROP which affects global state

SET allow_experimental_paimon_storage_engine = 1;
SET use_paimon_metadata_files_cache = 0;

DROP TABLE IF EXISTS paimon_cache_off;
DROP TABLE IF EXISTS paimon_cache_on;
DROP TEMPORARY TABLE IF EXISTS paimon_cache_events_before;

CREATE TEMPORARY TABLE paimon_cache_events_before
(
    hits UInt64,
    misses UInt64
)
ENGINE = Memory;

CREATE TABLE paimon_cache_off
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

TRUNCATE TABLE paimon_cache_events_before;
INSERT INTO paimon_cache_events_before
SELECT
    (SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheHits'),
    (SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheMisses');

SELECT count() FROM paimon_cache_off FORMAT Null;
SELECT count() FROM paimon_cache_off FORMAT Null;

SELECT
    (SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheHits') - (SELECT hits FROM paimon_cache_events_before),
    (SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheMisses') - (SELECT misses FROM paimon_cache_events_before);

DROP TABLE paimon_cache_off;

SET use_paimon_metadata_files_cache = 1;

CREATE TABLE paimon_cache_on
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

TRUNCATE TABLE paimon_cache_events_before;
INSERT INTO paimon_cache_events_before
SELECT
    (SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheHits'),
    (SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheMisses');

SELECT count() FROM paimon_cache_on FORMAT Null;
SELECT count() FROM paimon_cache_on FORMAT Null;

SELECT
    ((SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheHits') - (SELECT hits FROM paimon_cache_events_before)) > 0,
    ((SELECT value FROM system.events WHERE event = 'PaimonMetadataFilesCacheMisses') - (SELECT misses FROM paimon_cache_events_before)) > 0;

DROP TABLE paimon_cache_on;
DROP TEMPORARY TABLE paimon_cache_events_before;

SYSTEM DROP PAIMON METADATA CACHE;
