-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: stable physical codec inspection requires a Wide part.

DROP TABLE IF EXISTS t_tuple_codec_backup_source;
DROP TABLE IF EXISTS t_tuple_codec_backup_restored;

SET enable_tuple_element_codecs = 1;

CREATE TABLE t_tuple_codec_backup_source
(
    key UInt64,
    payload Tuple(
        overridden UInt64 CODEC(Delta, LZ4),
        inherited String,
        nested Tuple(
            score Float64 CODEC(Gorilla, ZSTD(1)),
            label String
        )
    ) CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0;

INSERT INTO t_tuple_codec_backup_source
SELECT
    number,
    (
        number * 3,
        concat('value-', toString(number % 10)),
        (number / 10, concat('label-', toString(number % 5)))
    )
FROM numbers(1000);

BACKUP TABLE t_tuple_codec_backup_source
TO Memory('05028_tuple_element_codecs_backup_restore') FORMAT Null;

SET enable_tuple_element_codecs = 0;

RESTORE TABLE t_tuple_codec_backup_source AS t_tuple_codec_backup_restored
FROM Memory('05028_tuple_element_codecs_backup_restore') FORMAT Null;

-- Restore replays persisted metadata, so it remains allowed while new declarations are disabled.
SELECT
    countSubstrings(create_table_query, 'CODEC(') = 3,
    position(create_table_query, 'overridden UInt64 CODEC(Delta(8), LZ4)') > 0,
    position(create_table_query, 'score Float64 CODEC(Gorilla(8), ZSTD(1))') > 0,
    position(create_table_query, 'CODEC(ZSTD(3))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_backup_restored';

DESCRIBE TABLE t_tuple_codec_backup_restored FORMAT JSONEachRow
SETTINGS describe_include_subcolumns = 1;

SELECT
    count() = 1000,
    sum(key) = 499500,
    sum(payload.overridden) = 1498500,
    uniqExact(payload.inherited) = 10
FROM t_tuple_codec_backup_restored;

INSERT INTO t_tuple_codec_backup_restored VALUES
    (1000, (3000, 'value-0', (100., 'label-0')));

-- The metadata uses the type-derived bit width, while the on-disk codec header uses bytes.
SELECT min(mapContains(codec_block_counts, 'Delta(1), LZ4'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_backup_restored)
WHERE column = 'payload' AND substream = 'payload%2Eoverridden';

DETACH TABLE t_tuple_codec_backup_restored;
ATTACH TABLE t_tuple_codec_backup_restored;

SELECT count() = 1001 FROM t_tuple_codec_backup_restored;

SELECT
    countSubstrings(create_table_query, 'CODEC(') = 3,
    position(create_table_query, 'overridden UInt64 CODEC(Delta(8), LZ4)') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_backup_restored';

DROP TABLE t_tuple_codec_backup_source;
DROP TABLE t_tuple_codec_backup_restored;
