#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage, no-darwin
# The shared `Nested` offsets stream is inspected as a raw local part file, so shared MergeTree and
# object storage (where the data lives remotely) are excluded. On a case-insensitive filesystem
# (macOS) `replaceFileNameToHashIfNeeded` hashes every stream file name regardless of
# `replace_long_file_name_to_hash`, so the `n.size0.bin` file cannot be located by name -- excluded
# via no-darwin. Full (non-packed) part storage is pinned in the table settings for the same reason:
# a packed part has no per-stream `.bin` files to inspect.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `ALTER TABLE ... RECOMPRESS COLUMN` of `Nested` siblings that share a single offsets stream
# (`share_nested_offsets = 1`) must choose the codec of the shared `n.size0` stream deterministically:
# the first sibling in stored (schema) order owns it, regardless of the order the columns are listed
# in the ALTER or of the internal hash-set iteration order. Here `n.a` is first, so the shared stream
# must take its codec (ZSTD, compression method byte 0x90) and never `n.b`'s (LZ4, 0x82).

shared_offsets_codec_byte()
{
    local part_path
    part_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_shared_codec' AND active LIMIT 1")
    local bin_file
    bin_file=$(ls "${part_path}"*.size0.bin 2>/dev/null | head -1)
    # Compressed block layout: 16-byte checksum, then the 1-byte compression method.
    od -An -tx1 -j16 -N1 "${bin_file}" | tr -d ' \n'
}

# Both column orders in the ALTER must yield the same codec for the shared stream: the schema-first
# sibling `n.a` (ZSTD), never the query-first or hash-first one.
for query_order in "\`n.a\`, RECOMPRESS COLUMN \`n.b\`" "\`n.b\`, RECOMPRESS COLUMN \`n.a\`"
do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_recompress_shared_codec"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE t_recompress_shared_codec
        (
            id UInt64,
            \`n.a\` Array(UInt64) CODEC(NONE),
            \`n.b\` Array(String) CODEC(NONE)
        )
        ENGINE = MergeTree ORDER BY id
        SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 replace_long_file_name_to_hash = 0,
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 min_level_for_full_part_storage = 0"

    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO t_recompress_shared_codec
        SELECT number, range(number % 5), arrayMap(x -> toString(x), range(number % 5)) FROM numbers(100000)"

    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_recompress_shared_codec MODIFY COLUMN \`n.a\` Array(UInt64) CODEC(ZSTD)"
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_recompress_shared_codec MODIFY COLUMN \`n.b\` Array(String) CODEC(LZ4)"
    ${CLICKHOUSE_CLIENT} --mutations_sync 2 --query "ALTER TABLE t_recompress_shared_codec RECOMPRESS COLUMN ${query_order}"

    echo "shared n.size0 codec byte: $(shared_offsets_codec_byte) (90 = ZSTD, from schema-first n.a)"
    ${CLICKHOUSE_CLIENT} --query "SELECT 'data intact', count(), countIf(n.a = arrayMap(x -> x, range(id % 5))), countIf(n.b = arrayMap(x -> toString(x), range(id % 5))) FROM t_recompress_shared_codec"
    ${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_recompress_shared_codec SETTINGS check_query_single_value_result = 1"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_recompress_shared_codec"
