#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every ABI in the runtime reads and writes the export named `memory`, so only that one decides
# whether a function can allocate in guest memory. A module may export several memories: an
# auxiliary `memory 0 0` beside a growable `memory` must not make a `BUFFERED_V1` function
# unusable, because the call never touches the auxiliary one.

MODULE="multi_memory_${CLICKHOUSE_DATABASE}"
FUNC="wasm_multi_memory_${CLICKHOUSE_DATABASE}"
PATCHED="${CLICKHOUSE_TMP}/text_split_multi_memory_${CLICKHOUSE_DATABASE}.wasm"

# Append a second, non-growable memory to the fixture and export it as `aux`.
python3 - "${CUR_DIR}/wasm/text_split_abi.wasm" "${PATCHED}" <<'EOF'
import sys

source, target = sys.argv[1], sys.argv[2]
data = open(source, 'rb').read()

def read_uleb(buf, pos):
    result, shift = 0, 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return result, pos

def write_uleb(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)

pos = 8
sections = []
while pos < len(data):
    section_id = data[pos]
    pos += 1
    length, pos = read_uleb(data, pos)
    sections.append([section_id, bytes(data[pos:pos + length])])
    pos += length

for section in sections:
    if section[0] == 5:
        count, offset = read_uleb(section[1], 0)
        # One more memory, declared as `memory 0 0`, so it can never grow.
        section[1] = write_uleb(count + 1) + section[1][offset:] + bytes([0x01, 0x00, 0x00])
    elif section[0] == 7:
        count, offset = read_uleb(section[1], 0)
        name = b'aux'
        # Export kind 0x02 is a memory; index 1 is the one appended above.
        section[1] = (write_uleb(count + 1) + section[1][offset:]
                      + write_uleb(len(name)) + name + b'\x02' + write_uleb(1))

out = b'\x00asm\x01\x00\x00\x00'
for section_id, payload in sections:
    out += bytes([section_id]) + write_uleb(len(payload)) + payload
open(target, 'wb').write(out)
EOF

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF

${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${PATCHED}"

${CLICKHOUSE_CLIENT} --query \
    "CREATE OR REPLACE FUNCTION ${FUNC}
        LANGUAGE WASM ABI BUFFERED_V1
        FROM '${MODULE}' :: 'batch_row_count'
        ARGUMENTS (x String) RETURNS UInt32
        SETTINGS serialization_format = 'CSV'"

echo 'An auxiliary non-growable memory does not disable the function'
${CLICKHOUSE_CLIENT} --query "SELECT ${FUNC}('abc')"

${CLICKHOUSE_CLIENT} << EOF
DROP FUNCTION IF EXISTS ${FUNC};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE}';
EOF
rm -f "${PATCHED}"
