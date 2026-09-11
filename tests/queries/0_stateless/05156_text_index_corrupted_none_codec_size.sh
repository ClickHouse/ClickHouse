#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A posting list segment of the 'none' codec is a Roaring bitmap prefixed with its size in bytes.
# The size comes from disk, so a corrupted part must fail with CORRUPTED_DATA before the decode buffer
# grows to the claimed size, both when a query reads the index and when a merge rebuilds it.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Every row has the token 'aaa'. The other tokens are unique and their postings are embedded into the dictionary.
# The postings file of the part thus holds one segment of 1000 row ids for 'aaa' at offset 0:
# the size of the bitmap as VarUInt, followed by the bitmap itself.
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
CREATE TABLE tab (id UInt32, str String, INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id
SETTINGS text_index_posting_list_codec = 'none';
INSERT INTO tab SELECT number, 'aaa u' || toString(number) FROM numbers(1000);
INSERT INTO tab SELECT number + 1000, 'aaa u' || toString(number + 1000) FROM numbers(1000);
"

part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY name LIMIT 1")
postings_file="${part_path}skp_idx_text_idx.pst.idx"

# Claims a bitmap of 16383 bytes: far above the size of any bitmap of 1000 row ids.
printf '\xff\x7f' | dd of="$postings_file" bs=1 seek=0 conv=notrunc status=none

# count() alone is answered from the token cardinality in the dictionary without reading the postings.
echo '-- query'
$CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT sum(id) FROM tab WHERE hasToken(str, 'aaa')" 2>&1 | grep -oE 'CORRUPTED_DATA|CANNOT_READ_ALL_DATA' | head -1

echo '-- merge'
$CLICKHOUSE_LOCAL --path "$data_path" -q "OPTIMIZE TABLE tab FINAL" 2>&1 | grep -oE 'CORRUPTED_DATA|CANNOT_READ_ALL_DATA' | head -1
$CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active"

rm -rf "${data_path:?}"
