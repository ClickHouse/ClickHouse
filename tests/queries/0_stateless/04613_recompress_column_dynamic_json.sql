-- `ALTER TABLE ... RECOMPRESS COLUMN` on a column with a data-dependent dynamic structure
-- (`Dynamic`, `JSON`) must recompress *all* of the column's streams, not just `dynamic_structure`.
--
-- The in-place wide-part fast path enumerates a column's streams with a state-less
-- `serialization->enumerateStreams`, which stops after `dynamic_structure` for `SerializationDynamic`
-- (no sample column / deserialize state) and so cannot see the real variant/data substreams. To stay
-- correct, `splitAndModifyMutationCommands` routes any command referencing a `hasDynamicSubcolumns`
-- column -- including `RECOMPRESS COLUMN` -- through `haveMutationsOfDynamicColumns` to the whole-part
-- rewrite, which re-serializes the column with its current codec. This test guards that the whole
-- column (not just its structure stream) is actually recompressed.

SET mutations_sync = 2;
SET check_query_single_value_result = 1;
SET allow_experimental_dynamic_type = 1;
SET allow_experimental_json_type = 1;

-- Dynamic column: highly compressible payload, stored uncompressed at first.
DROP TABLE IF EXISTS t_recompress_dynamic;
CREATE TABLE t_recompress_dynamic (id UInt64, d Dynamic CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_dynamic SELECT number, repeat('a', 200)::Dynamic FROM numbers(100000);

SELECT DISTINCT 'dynamic wide part', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_dynamic' AND active;
SELECT 'dynamic none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_dynamic' AND column = 'd' AND active;

-- Change the codec (metadata-only), then apply it to existing data. If only `dynamic_structure` were
-- recompressed, the variant/data streams would stay under `NONE` and the size would not shrink.
ALTER TABLE t_recompress_dynamic MODIFY COLUMN d Dynamic CODEC(ZSTD(3));
ALTER TABLE t_recompress_dynamic RECOMPRESS COLUMN d;

SELECT 'dynamic zstd is small', sum(data_compressed_bytes) < 1500000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_dynamic' AND column = 'd' AND active;
SELECT 'dynamic point', d::String = repeat('a', 200) FROM t_recompress_dynamic WHERE id = 99999;
CHECK TABLE t_recompress_dynamic;

DROP TABLE t_recompress_dynamic;

-- Same for a `JSON` column.
DROP TABLE IF EXISTS t_recompress_json;
CREATE TABLE t_recompress_json (id UInt64, j JSON CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_json SELECT number, ('{"a":"' || repeat('x', 150) || '","b":' || toString(number) || '}')::JSON FROM numbers(100000);

SELECT DISTINCT 'json wide part', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_json' AND active;
SELECT 'json none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_json' AND column = 'j' AND active;

ALTER TABLE t_recompress_json MODIFY COLUMN j JSON CODEC(ZSTD(3));
ALTER TABLE t_recompress_json RECOMPRESS COLUMN j;

SELECT 'json zstd is small', sum(data_compressed_bytes) < 3000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_json' AND column = 'j' AND active;
SELECT 'json point', j.a = repeat('x', 150), j.b = 99999 FROM t_recompress_json WHERE id = 99999;
CHECK TABLE t_recompress_json;

DROP TABLE t_recompress_json;
