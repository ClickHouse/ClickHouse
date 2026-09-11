#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge of text indexes decodes the posting lists of the source parts segment by segment.
# The segment headers come from disk, so a corrupted part must fail the merge with CORRUPTED_DATA
# instead of growing the decode buffers to the sizes claimed by the corrupted header.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Every row has the token 'aaa'. The other tokens are unique and their postings are embedded into the dictionary.
# The postings file of the part thus holds one bitpacking segment of 1000 row ids for 'aaa' at offset 0:
# codec type at offset 0, payload bytes at 1-2, cardinality 1000 at 3-4, first row id 0 at 5,
# bit width of the first block at 6 and its deltas at 7-22, followed by the other blocks and the block index.
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
CREATE TABLE tab (id UInt32, str String, INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id
SETTINGS text_index_posting_list_codec = 'bitpacking';
INSERT INTO tab SELECT number, 'aaa u' || toString(number) FROM numbers(1000);
INSERT INTO tab SELECT number + 1000, 'aaa u' || toString(number + 1000) FROM numbers(1000);
"

part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY name LIMIT 1")
postings_file="${part_path}skp_idx_text_idx.pst.idx"
cp "$postings_file" "${data_path}/postings.bak"

# Overwrites the bytes at the offset of the postings file and tries to merge the parts.
function corrupt_and_merge()
{
    local name=$1 offset=$2 bytes=$3

    cp "${data_path}/postings.bak" "$postings_file"
    # shellcheck disable=SC2059
    printf "$bytes" | dd of="$postings_file" bs=1 seek="$offset" conv=notrunc status=none

    echo "-- $name"
    $CLICKHOUSE_LOCAL --path "$data_path" -q "OPTIMIZE TABLE tab FINAL" 2>&1 | grep -oE 'CORRUPTED_DATA|INCORRECT_DATA|LOGICAL_ERROR' | head -1
    $CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active"
}

corrupt_and_merge 'payload bytes beyond the bound for the cardinality' 1 '\xff\x7f'
corrupt_and_merge 'segment cardinality beyond the token cardinality' 3 '\xe9\x07'

echo '-- the intact postings merge'
cp "${data_path}/postings.bak" "$postings_file"
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
OPTIMIZE TABLE tab FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;
SELECT count() FROM tab WHERE hasToken(str, 'aaa');
"

rm -rf "${data_path:?}"
