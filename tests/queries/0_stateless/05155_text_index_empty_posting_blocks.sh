#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

for positions in 0 1; do
    echo "-- positions: $positions"
    $CLICKHOUSE_LOCAL --path "$data_path" -m -q "
        CREATE TABLE tab
        (
            id UInt32, str String,
            INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = $positions)
        )
        ENGINE = MergeTree ORDER BY id
        SETTINGS text_index_posting_list_codec = 'bitpacking',
            text_index_posting_list_block_size = 512,
            text_index_dictionary_block_frontcoding_compression = 0,
            allow_experimental_text_index_phrase_search = 1;
        INSERT INTO tab SELECT number, if(number < 1000, 'aaa', 'zzz') FROM numbers(2000);
        INSERT INTO tab SELECT number + 2000, 'aaa' FROM numbers(1000);
    "

    part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "
        SELECT path FROM system.parts
        WHERE database = currentDatabase() AND table = 'tab' AND active
        ORDER BY name LIMIT 1")
    dictionary_file="${part_path}skp_idx_text_idx.dct.idx"
    checksums_file="${part_path}checksums.txt"
    cp "$dictionary_file" "${data_path}/dictionary.bak"
    cp "$checksums_file" "${data_path}/checksums.bak"
    $CLICKHOUSE_COMPRESSOR --decompress < "$dictionary_file" > "${data_path}/dictionary.raw"

    # Keep the valid metadata for `aaa`, then replace the block count of `zzz` with zero.
    # `zzz` only occurs in one source and reuses a merge cursor previously used by `aaa`.
    python3 - "${data_path}/dictionary.raw" <<'PY'
import io
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = path.read_bytes()
stream = io.BytesIO(data)


def read_var_uint():
    value = 0
    shift = 0
    while True:
        byte = stream.read(1)[0]
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value
        shift += 7


assert read_var_uint() == 0  # Raw strings.
assert read_var_uint() == 2
assert [stream.read(read_var_uint()) for _ in range(2)] == [b"aaa", b"zzz"]

for token in (b"aaa", b"zzz"):
    header = read_var_uint()
    assert read_var_uint() == 1000
    assert not header & ((1 << 1) | (1 << 2))  # No embedded postings or single block.
    if header & (1 << 5):  # Positions.
        read_var_uint()
        read_var_uint()
    count_offset = stream.tell()
    num_blocks = read_var_uint()
    assert num_blocks > 1
    if token == b"zzz":
        path.write_bytes(data[:count_offset] + b"\x00")
        break
    for _ in range(num_blocks):
        read_var_uint()  # Offset.
        read_var_uint()  # First row.
        read_var_uint()  # Last row.
PY

    $CLICKHOUSE_COMPRESSOR < "${data_path}/dictionary.raw" > "$dictionary_file"
    # The rewritten file has a different size and would be detected as a broken part on start.
    # Without checksums.txt the part loader recomputes the checksums from the files on disk.
    rm "$checksums_file"
    if $CLICKHOUSE_LOCAL --path "$data_path" -q "OPTIMIZE TABLE tab FINAL" > "${data_path}/merge.log" 2>&1; then
        echo 'Unexpected successful merge of corrupted postings'
        exit 1
    fi
    grep -o -m 1 'CORRUPTED_DATA' "${data_path}/merge.log"
    $CLICKHOUSE_LOCAL --path "$data_path" -q "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'tab' AND active"

    cp "${data_path}/dictionary.bak" "$dictionary_file"
    cp "${data_path}/checksums.bak" "$checksums_file"
    $CLICKHOUSE_LOCAL --path "$data_path" -m -q "
        OPTIMIZE TABLE tab FINAL;
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'tab' AND active;
        SELECT count(), min(id), max(id) FROM tab WHERE hasToken(str, 'zzz');
    "
    rm -rf "${data_path:?}"
done
