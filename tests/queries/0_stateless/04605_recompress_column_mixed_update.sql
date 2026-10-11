-- Tags: no-shared-merge-tree
-- Mixed mutation: one `Nested` sibling is rewritten (UPDATE) while another is recompressed
-- (RECOMPRESS COLUMN) in the same ALTER. With `share_nested_offsets` the siblings `n.a`/`n.b` share
-- one `n.size0` offsets stream. RECOMPRESS COLUMN of the recompressed sibling must still apply its
-- codec to that sibling's own streams, and the shared offsets stream must be written exactly once,
-- by the mutation's column output stream for the updated sibling (matching a fresh write) rather
-- than being rewritten by the recompression and overwritten again by the output stream. The part
-- must stay consistent and every row must be intact.
-- The shared MergeTree data lives remotely, so this raw on-disk behaviour is checked on local parts.

DROP TABLE IF EXISTS t_recompress_mixed;

CREATE TABLE t_recompress_mixed
(
    id UInt64,
    `n.a` Array(UInt64) CODEC(NONE),
    `n.b` Array(String) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_mixed
SELECT number, range(number % 5), arrayMap(x -> repeat('x', 50), range(number % 5)) FROM numbers(100000);

-- Give `n.b` a compressible codec. This changes only the metadata; the stored data keeps its old
-- uncompressed representation until it is recompressed.
ALTER TABLE t_recompress_mixed MODIFY COLUMN `n.b` Array(String) CODEC(ZSTD);

SELECT 'n.b still uncompressed after MODIFY', max(column_data_compressed_bytes) > 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_mixed' AND active AND column = 'n.b';

-- Rewrite `n.a` (which also rewrites the shared offsets stream) and recompress `n.b` at once.
ALTER TABLE t_recompress_mixed UPDATE `n.a` = `n.a` WHERE 1, RECOMPRESS COLUMN `n.b`
SETTINGS mutations_sync = 2;

-- RECOMPRESS COLUMN `n.b` must have applied ZSTD to `n.b`'s own value stream even though the shared
-- offsets stream is owned by the updated sibling.
SELECT 'n.b recompressed after mixed mutation', max(column_data_compressed_bytes) < 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_mixed' AND active AND column = 'n.b';

SELECT 'data intact after UPDATE n.a + RECOMPRESS n.b', count(),
    countIf(`n.a` = arrayMap(x -> x, range(id % 5))),
    countIf(`n.b` = arrayMap(x -> repeat('x', 50), range(id % 5)))
FROM t_recompress_mixed;

CHECK TABLE t_recompress_mixed SETTINGS check_query_single_value_result = 1;

DROP TABLE t_recompress_mixed;

-- The reverse mix: update the sibling that shares the offsets stream and recompress the other one.
CREATE TABLE t_recompress_mixed
(
    id UInt64,
    `n.a` Array(UInt64) CODEC(NONE),
    `n.b` Array(String) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_mixed
SELECT number, arrayMap(x -> 12345, range(number % 5)), arrayMap(x -> toString(x), range(number % 5)) FROM numbers(100000);

ALTER TABLE t_recompress_mixed MODIFY COLUMN `n.a` Array(UInt64) CODEC(ZSTD);

SELECT 'n.a still uncompressed after MODIFY', max(column_data_compressed_bytes) > 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_mixed' AND active AND column = 'n.a';

ALTER TABLE t_recompress_mixed UPDATE `n.b` = `n.b` WHERE 1, RECOMPRESS COLUMN `n.a`
SETTINGS mutations_sync = 2;

SELECT 'n.a recompressed after mixed mutation', max(column_data_compressed_bytes) < 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_mixed' AND active AND column = 'n.a';

SELECT 'data intact after UPDATE n.b + RECOMPRESS n.a', count(),
    countIf(`n.a` = arrayMap(x -> toUInt64(12345), range(id % 5))),
    countIf(`n.b` = arrayMap(x -> toString(x), range(id % 5)))
FROM t_recompress_mixed;

CHECK TABLE t_recompress_mixed SETTINGS check_query_single_value_result = 1;

DROP TABLE t_recompress_mixed;
