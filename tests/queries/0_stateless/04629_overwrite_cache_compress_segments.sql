DROP TABLE IF EXISTS overwrite_cache_compressed;
DROP TABLE IF EXISTS overwrite_cache_plain;
DROP TABLE IF EXISTS overwrite_cache_source;

CREATE TABLE overwrite_cache_source ENGINE = Memory AS
SELECT
    toUInt8(number % 5) AS website_type,
    toUInt32(number % 3000) AS user_id,
    concat('tag', toString(number % 7))::LowCardinality(String) AS tag,
    toUInt64(number % 40) AS version,
    toUInt64(number) AS source_sequence,
    repeat('payload', 1 + number % 3) AS value
FROM numbers(50000);

CREATE TABLE overwrite_cache_plain
(
    website_type UInt8,
    user_id UInt32,
    tag LowCardinality(String),
    version UInt64,
    source_sequence UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag), (website_type), (website_type, tag)
SETTINGS max_memory_bytes = 1073741824, equal_version_tiebreak_columns = 'source_sequence';

CREATE TABLE overwrite_cache_compressed
(
    website_type UInt8,
    user_id UInt32,
    tag LowCardinality(String),
    version UInt64,
    source_sequence UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag), (website_type), (website_type, tag)
SETTINGS max_memory_bytes = 1073741824, equal_version_tiebreak_columns = 'source_sequence', compress_segments = 1;

INSERT INTO overwrite_cache_plain SELECT * FROM overwrite_cache_source
SETTINGS max_insert_block_size = 4000, min_insert_block_size_rows = 4000, min_insert_block_size_bytes = 0;
INSERT INTO overwrite_cache_compressed SELECT * FROM overwrite_cache_source
SETTINGS max_insert_block_size = 4000, min_insert_block_size_rows = 4000, min_insert_block_size_bytes = 0;

-- Compression must change neither which row wins nor the stored payload.
SELECT 'winners agree with the reference';
WITH reference AS
(
    SELECT
        website_type,
        user_id,
        tag,
        argMax(version, (version, source_sequence)) AS winner_version,
        argMax(source_sequence, (version, source_sequence)) AS winner_sequence,
        argMax(value, (version, source_sequence)) AS winner_value
    FROM overwrite_cache_source
    GROUP BY website_type, user_id, tag
)
SELECT
    (SELECT count() FROM reference) = (SELECT count() FROM overwrite_cache_plain WHERE website_type IN (0, 1, 2, 3, 4)),
    (SELECT count() FROM reference) = (SELECT count() FROM overwrite_cache_compressed WHERE website_type IN (0, 1, 2, 3, 4)),
    (SELECT count() FROM (SELECT * FROM overwrite_cache_plain WHERE website_type IN (0, 1, 2, 3, 4) EXCEPT SELECT * FROM reference)),
    (SELECT count() FROM (SELECT * FROM overwrite_cache_compressed WHERE website_type IN (0, 1, 2, 3, 4) EXCEPT SELECT * FROM reference));

SELECT 'lookup indexes agree';
SELECT
    (SELECT count() FROM overwrite_cache_plain WHERE tag = 'tag3') = (SELECT count() FROM overwrite_cache_compressed WHERE tag = 'tag3'),
    (SELECT sum(cityHash64(value)) FROM overwrite_cache_plain WHERE website_type = 2 AND tag = 'tag3')
        = (SELECT sum(cityHash64(value)) FROM overwrite_cache_compressed WHERE website_type = 2 AND tag = 'tag3');

SELECT 'point lookups agree';
SELECT countIf(
    overwriteCacheGetOrNull(concat(currentDatabase(), '.overwrite_cache_plain'), 'value', website_type, user_id, tag)
    != overwriteCacheGetOrNull(concat(currentDatabase(), '.overwrite_cache_compressed'), 'value', website_type, user_id, tag))
FROM overwrite_cache_plain
WHERE website_type IN (0, 1, 2, 3, 4);

-- Replacing every key retires and compacts segments in both representations.
SELECT 'repeated replacement';
CREATE TABLE overwrite_cache_replacement ENGINE = Memory AS
SELECT DISTINCT website_type, user_id, tag, toUInt64(100) AS version, toUInt64(0) AS source_sequence, 'final' AS value
FROM overwrite_cache_source;
INSERT INTO overwrite_cache_plain SELECT * FROM overwrite_cache_replacement
SETTINGS max_insert_block_size = 4000, min_insert_block_size_rows = 4000, min_insert_block_size_bytes = 0;
INSERT INTO overwrite_cache_compressed SELECT * FROM overwrite_cache_replacement
SETTINGS max_insert_block_size = 4000, min_insert_block_size_rows = 4000, min_insert_block_size_bytes = 0;
SELECT countIf(value != 'final'), countIf(version != 100)
FROM overwrite_cache_plain WHERE website_type IN (0, 1, 2, 3, 4);
SELECT countIf(value != 'final'), countIf(version != 100)
FROM overwrite_cache_compressed WHERE website_type IN (0, 1, 2, 3, 4);

SELECT 'rejects an unsupported value';
CREATE TABLE overwrite_cache_bad (key UInt64, version UInt64)
ENGINE = OverwriteCache(version) KEYS (key)
SETTINGS max_memory_bytes = 1048576, compress_segments = 'yes'; -- { serverError BAD_ARGUMENTS }

DROP TABLE overwrite_cache_replacement;
DROP TABLE overwrite_cache_compressed;
DROP TABLE overwrite_cache_plain;
DROP TABLE overwrite_cache_source;
