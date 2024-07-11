DROP TABLE IF EXISTS default_compression_codec_table;

-- Case 1
CREATE TABLE default_compression_codec_table (
    some_key UInt64 CODEC(DoubleDelta, LZ4), 
    some_id UInt32, 
    some_type String, 
    some_value Float64 CODEC(Gorilla)
) ENGINE = MergeTree()
ORDER BY (some_key)
SETTINGS default_compression_codec = 'LZ4';

SELECT
    name,
    compression_codec
FROM system.columns
WHERE table = 'default_compression_codec_table';

DROP TABLE default_compression_codec_table;

-- Case 2
CREATE TABLE default_compression_codec_table (
    some_key UInt64, 
    some_id UInt32, 
    some_type String, 
    some_value Float64
) ENGINE = MergeTree()
ORDER BY (some_key)
SETTINGS default_compression_codec = 'LZ4';

SELECT
    name,
    compression_codec
FROM system.columns
WHERE table = 'default_compression_codec_table';

DROP TABLE default_compression_codec_table;

-- Case 3
CREATE TABLE default_compression_codec_table (
    some_key UInt64 CODEC(ZSTD), 
    some_id UInt32 CODEC(ZSTD), 
    some_type String CODEC(ZSTD), 
    some_value Float64 CODEC(ZSTD)
) ENGINE = MergeTree()
ORDER BY (some_key)
SETTINGS default_compression_codec = 'LZ4';

SELECT
    name,
    compression_codec
FROM system.columns
WHERE table = 'default_compression_codec_table';

DROP TABLE default_compression_codec_table;

-- Case 4
CREATE TABLE default_compression_codec_table (
    some_key UInt64 , 
    some_id UInt32 CODEC(ZSTD), 
    some_type String CODEC(LZ4), 
    some_value Float64 CODEC(Gorilla)
) ENGINE = MergeTree()
ORDER BY (some_key)
SETTINGS default_compression_codec = 'Delta(8), LZ4';

SELECT
    name,
    compression_codec
FROM system.columns
WHERE table = 'default_compression_codec_table';

DROP TABLE default_compression_codec_table;

-- Case 5
CREATE TABLE default_compression_codec_table (
    some_key UInt64, 
    some_id UInt32, 
    some_type String, 
    some_value Float64
) ENGINE = MergeTree()
ORDER BY (some_key)
SETTINGS default_compression_codec = 'ZSTD(2)';

SELECT
    name,
    compression_codec
FROM system.columns
WHERE table = 'default_compression_codec_table';

DROP TABLE default_compression_codec_table;