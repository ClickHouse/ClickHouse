#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: text indexes are not available in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A damaged posting list of a text index must be rejected while reading it: its header sizes the
# read buffers and its row ids go into the query as matching rows, so both are checked against the
# token metadata of the dictionary.

WORKING_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

READ_POSTINGS="SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1"

# Rewrites the VarUInt number `$2` (0-based) of the posting list stream `$1` with the value `$3`.
patch_varint()
{
    python3 -c "
import sys

path, index, value = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
data = bytearray(open(path, 'rb').read())
out = bytearray()
pos = 0

for i in range(index + 1):
    number = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        number |= (byte & 0x7f) << shift
        shift += 7
        if not byte & 0x80:
            break

    number = value if i == index else number
    while True:
        byte = number & 0x7f
        number >>= 7
        out.append(byte | 0x80 if number else byte)
        if not number:
            break

open(path, 'wb').write(bytes(out) + bytes(data[pos:]))
" "$1" "$2" "$3"
}

# Builds a table with a single indexed token, damages one number of its posting list and reads it.
# $1 - table, $2 - posting list codec, $3 - number to patch, $4 - value, $5 - expected message.
run_case()
{
    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "
        CREATE TABLE $1 (id UInt32, s String, INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = '$2', posting_list_block_size = 1024))
        ENGINE = MergeTree ORDER BY id
        -- min_bytes_for_full_part_storage=0: a packed part keeps the stream edited below inside data.packed.
        SETTINGS min_bytes_for_full_part_storage = 0;
        INSERT INTO $1 SELECT number, 'foo' FROM numbers(100);
        SELECT sum(id) FROM $1 WHERE hasToken(s, 'foo') ${READ_POSTINGS};
    "

    local part
    part=$(${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "SELECT path FROM system.parts WHERE table = '$1' AND active")

    if [ ! -f "${part}skp_idx_idx.pst.idx" ]
    then
        echo "no postings file in ${part}:" >&2
        ls "${part}" >&2
        exit 1
    fi

    patch_varint "${part}skp_idx_idx.pst.idx" "$3" "$4"
    # The checksums are recalculated on the next load.
    rm -f "${part}checksums.txt"

    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "
        SELECT sum(id) FROM $1 WHERE hasToken(s, 'foo') ${READ_POSTINGS}" 2>&1 | grep -m1 -o -F "$5"
}

echo '-- the declared size of an uncompressed posting list'
run_case none_bitmap_size none 0 2000 'bitmap of 2000 bytes exceeds the upper bound'

echo '-- the declared number of row ids of a compressed posting list'
run_case bitpacking_cardinality bitpacking 2 0 'cardinality 0 is not in the range'

echo '-- the first row id of a compressed posting list, which shifts all of its row ids'
run_case bitpacking_first_row_id bitpacking 3 5 'row ids from 5 to 104 while its row range is [0, 99]'

rm -rf "${WORKING_DIR:?}"
