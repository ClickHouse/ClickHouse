-- Tags: no-shared-merge-tree
-- Mixed mutation where a newly added `Nested` sibling is materialized in the same ALTER that
-- recompresses another sibling. With `share_nested_offsets` the siblings share one `n.size0` offsets
-- stream. The new sibling `n.c` is added by `ADD COLUMN` after the part was written, so old parts do
-- not have it yet, but its column output stream (materialization) still writes the shared `n.size0`.
-- The shared-stream pre-seeding must therefore also cover updated columns that are absent from the
-- source part: the recompression of the other sibling must not rewrite `n.size0` only for the
-- materialization to overwrite it again. RECOMPRESS COLUMN of the recompressed sibling must still
-- apply its codec to that sibling's own value stream, the materialized sibling must get its default
-- values, the part must stay consistent and every row must be intact.
-- The shared MergeTree data lives remotely, so this raw on-disk behaviour is checked on local parts.

DROP TABLE IF EXISTS t_recompress_new_sibling;

CREATE TABLE t_recompress_new_sibling
(
    id UInt64,
    `n.a` Array(UInt64) CODEC(NONE),
    `n.b` Array(String) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_new_sibling
SELECT number, range(number % 5), arrayMap(x -> repeat('x', 50), range(number % 5)) FROM numbers(100000);

-- Give `n.b` a compressible codec. This changes only the metadata; the stored data keeps its old
-- uncompressed representation until it is recompressed.
ALTER TABLE t_recompress_new_sibling MODIFY COLUMN `n.b` Array(String) CODEC(ZSTD);

-- Add a new sibling to the same `Nested`. Old parts still lack `n.c`, so materializing it exercises
-- the "updated column not present in the source part" path. Its DEFAULT produces arrays of the same
-- length as the existing siblings, as required by the shared offsets.
ALTER TABLE t_recompress_new_sibling ADD COLUMN `n.c` Array(UInt64) DEFAULT arrayMap(x -> x + 1, range(id % 5)) CODEC(ZSTD);

SELECT 'n.b still uncompressed before RECOMPRESS', max(column_data_compressed_bytes) > 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_new_sibling' AND active AND column = 'n.b';

-- Materialize the new sibling (writes the shared offsets stream) and recompress the other sibling at
-- once.
ALTER TABLE t_recompress_new_sibling MATERIALIZE COLUMN `n.c`, RECOMPRESS COLUMN `n.b`
SETTINGS mutations_sync = 2;

-- RECOMPRESS COLUMN `n.b` must have applied ZSTD to `n.b`'s own value stream even though the shared
-- offsets stream is owned by the materialized sibling.
SELECT 'n.b recompressed after mixed mutation', max(column_data_compressed_bytes) < 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_new_sibling' AND active AND column = 'n.b';

SELECT 'data intact after MATERIALIZE n.c + RECOMPRESS n.b', count(),
    countIf(`n.a` = arrayMap(x -> x, range(id % 5))),
    countIf(`n.b` = arrayMap(x -> repeat('x', 50), range(id % 5))),
    countIf(`n.c` = arrayMap(x -> x + 1, range(id % 5)))
FROM t_recompress_new_sibling;

CHECK TABLE t_recompress_new_sibling SETTINGS check_query_single_value_result = 1;

DROP TABLE t_recompress_new_sibling;

-- The reverse ALTER order: recompress first, then materialize the new sibling.
CREATE TABLE t_recompress_new_sibling
(
    id UInt64,
    `n.a` Array(UInt64) CODEC(NONE),
    `n.b` Array(String) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_new_sibling
SELECT number, range(number % 5), arrayMap(x -> repeat('x', 50), range(number % 5)) FROM numbers(100000);

ALTER TABLE t_recompress_new_sibling MODIFY COLUMN `n.b` Array(String) CODEC(ZSTD);
ALTER TABLE t_recompress_new_sibling ADD COLUMN `n.c` Array(UInt64) DEFAULT arrayMap(x -> x + 1, range(id % 5)) CODEC(ZSTD);

ALTER TABLE t_recompress_new_sibling RECOMPRESS COLUMN `n.b`, MATERIALIZE COLUMN `n.c`
SETTINGS mutations_sync = 2;

SELECT 'n.b recompressed after reversed mixed mutation', max(column_data_compressed_bytes) < 1000000
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_new_sibling' AND active AND column = 'n.b';

SELECT 'data intact after RECOMPRESS n.b + MATERIALIZE n.c', count(),
    countIf(`n.a` = arrayMap(x -> x, range(id % 5))),
    countIf(`n.b` = arrayMap(x -> repeat('x', 50), range(id % 5))),
    countIf(`n.c` = arrayMap(x -> x + 1, range(id % 5)))
FROM t_recompress_new_sibling;

CHECK TABLE t_recompress_new_sibling SETTINGS check_query_single_value_result = 1;

DROP TABLE t_recompress_new_sibling;
