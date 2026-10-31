#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Each file holds one deletion-vector-v1 blob with a single roaring bitmap at key 0.
python3 - "$TMP" <<'PY'
import json, struct, sys, zlib

def write_puffin(path, bitmap, cardinality):
    magic = b"PFA1"
    body = bytes([0xD1, 0xD3, 0x39, 0x64]) + struct.pack("<qi", 1, 0) + bitmap
    blob = struct.pack(">I", len(body)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)
    footer = {"blobs": [{"type": "deletion-vector-v1", "fields": [], "snapshot-id": -1,
        "sequence-number": -1, "offset": 4, "length": len(blob),
        "properties": {"referenced-data-file": "f.parquet", "cardinality": str(cardinality)}}]}
    fj = json.dumps(footer, separators=(", ", ": ")).encode()
    with open(path, "wb") as f:
        f.write(magic + blob + magic + fj + struct.pack("<i", len(fj)) + b"\x00\x00\x00\x00" + magic)

d = sys.argv[1]
# Run container with zero runs.
write_puffin(d + "/zero_runs.puffin",
    struct.pack("<I", 12347) + b"\x01" + struct.pack("<HHH", 0, 0, 0), 0)
# Bitset container whose stored cardinality (4097) does not match its single set bit.
write_puffin(d + "/bitset_wrong_cardinality.puffin",
    struct.pack("<II", 12346, 1) + struct.pack("<HHI", 0, 4096, 16) + struct.pack("<Q", 1) + b"\x00" * 8184, 4097)
# Valid run container [10, 14].
write_puffin(d + "/valid_run.puffin",
    struct.pack("<I", 12347) + b"\x01" + struct.pack("<HH", 0, 4) + struct.pack("<HHH", 1, 10, 4), 5)
PY

for name in zero_runs bitset_wrong_cardinality
do
    echo "--- $name ---"
    err=$($CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$TMP/$name.puffin', Puffin)" 2>&1) || true
    echo "$err" | grep -oF 'Failed to deserialize deletion vector roaring bitmap' || true
    echo "$err" | grep -oF 'BAD_ARGUMENTS' || true
done

echo "--- valid_run ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$TMP/valid_run.puffin', Puffin)"
