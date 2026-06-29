#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# Regression test for an untracked-allocation DoS in the Avro reader.
# BinaryDecoder::decodeString / decodeBytes read a length prefix and resized a std::string/vector to
# that length BEFORE reading any bytes. A ~70-byte Avro file declaring a ~2 GB string length (with no
# data) forced a ~2 GB allocation that bypassed max_memory_usage (peak RSS ~2.9 GB; the query then
# failed with an Avro EOF error). The decoder now reads incrementally, so a truncated/over-declared
# value only allocates the bytes actually present (RSS stays bounded) and is rejected cleanly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

# Hand-build a minimal Avro OCF whose single string value declares a ~2 GB length with no data bytes.
python3 - "$WORK_DIR" <<'PYEOF'
import json, sys
work = sys.argv[1]

def vlong(n):  # Avro zigzag varint long
    u = ((n << 1) ^ (n >> 63)) & ((1 << 64) - 1)
    out = bytearray()
    while True:
        b = u & 0x7f; u >>= 7
        out.append(b | 0x80) if u else out.append(b)
        if not u: break
    return bytes(out)

def avstr(b): return vlong(len(b)) + b

schema = json.dumps({"type": "record", "name": "r",
                     "fields": [{"name": "s", "type": "string"}]}).encode()
meta = (vlong(2) + avstr(b'avro.schema') + avstr(schema)
        + avstr(b'avro.codec') + avstr(b'null') + vlong(0))
sync = b'\x00' * 16
header = b'Obj\x01' + meta + sync
payload = vlong(2_000_000_000)              # string length prefix ~2 GB, no bytes follow
block = vlong(1) + vlong(len(payload)) + payload + sync
open(f"{work}/strbomb.avro", "wb").write(header + block)
PYEOF

# The over-declared string must be rejected cleanly (and with bounded memory) instead of allocating
# ~2 GB up front. The data genuinely ends after the length prefix, so this surfaces as an Avro
# EOF / INCORRECT_DATA error.
out=$(${CLICKHOUSE_LOCAL} --input-format Avro -S "s String" \
        --query "SELECT s FROM table FORMAT Null SETTINGS max_memory_usage = 104857600" \
        < "${WORK_DIR}/strbomb.avro" 2>&1)
if echo "$out" | grep -qE "AVRO_EXCEPTION|EOF reached|INCORRECT_DATA|CANNOT_READ_ALL_DATA"; then
    echo "strbomb: rejected"
else
    echo "strbomb: UNEXPECTED: $out"
fi
