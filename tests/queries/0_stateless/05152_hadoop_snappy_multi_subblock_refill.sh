#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on Snappy

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A hadoop-snappy block declares its total uncompressed length and holds one or more subblocks, each
# with its own compressed length. The file below has a second block of two subblocks, so an input
# refill can fall between them - which used to drop the subblocks decoded before the refill.
FILE="${CLICKHOUSE_TMP}/05152_boundary.snappy"

python3 - "$FILE" <<'PYEOF'
import struct
import sys

def raw_snappy_literals(payload):
    """A raw snappy stream of literal-only copies, so no snappy library is needed to write it."""
    out = bytearray()
    n = len(payload)
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    i = 0
    while i < len(payload):
        segment = payload[i:i + 60]
        out.append((len(segment) - 1) << 2)
        out += segment
        i += 60
    return bytes(out)

c, a, b = b'C' * 500_000, b'A' * 300_000, b'B' * 300_000
cc, ca, cb = raw_snappy_literals(c), raw_snappy_literals(a), raw_snappy_literals(b)

one_subblock = struct.pack('>I', len(c)) + struct.pack('>I', len(cc)) + cc
two_subblocks = (struct.pack('>I', len(a) + len(b))
                 + struct.pack('>I', len(ca)) + ca
                 + struct.pack('>I', len(cb)) + cb)

with open(sys.argv[1], 'wb') as f:
    f.write(one_subblock + two_subblocks)
PYEOF

QUERY="SELECT length(raw), countSubstrings(raw, 'A'), countSubstrings(raw, 'B'), countSubstrings(raw, 'C')"

# From a local file the whole stream fits in one read buffer.
$CLICKHOUSE_LOCAL --query "$QUERY FROM file('$FILE', 'RawBLOB', 'raw String', 'snappy')"

# Through a pipe the input is refilled in the middle of the two-subblock block.
cat "$FILE" | $CLICKHOUSE_LOCAL --query "$QUERY FROM file('-', 'RawBLOB', 'raw String', 'snappy')"

rm -f "$FILE"
