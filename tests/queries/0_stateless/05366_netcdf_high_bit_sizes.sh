#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The number of records of CDF-1 and CDF-2 is a 32-bit `NON_NEG` field, of which only the all-ones
# value is reserved as the streaming marker. Every other value above the maximum of `Int32` is a
# negative count and must be rejected instead of being read as a huge number of records.

# One unlimited dimension `t` and one record variable `int v(t)`. The number of records is the
# argument. The header of CDF-2 stores sizes as 32 bits and offsets as 64 bits, so it is 84 bytes.
write_file()
{
    python3 - "$1" "$2" "${3:-0}" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

header = b'CDF\x02' + struct.pack('>I', int(sys.argv[2], 0))
header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
header += ABSENT
header += tag(NC_VARIABLE) + tag(1)
header += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + struct.pack('>q', 84)
assert len(header) == 84, len(header)

num_data_records = int(sys.argv[3])
with open(sys.argv[1], 'wb') as out:
    out.write(header + b''.join(struct.pack('>i', value) for value in range(num_data_records)))
PYTHON
}

echo "--- 0xFFFFFFFE records in the header of a CDF-2 file is a negative count"
write_file "$FILE" 0xFFFFFFFE
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF)" 2>&1 |
    grep -o -m1 "Negative number of records in the NetCDF header"
rm -f "$FILE"

echo "--- and so is 0x80000000"
write_file "$FILE" 0x80000000
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 |
    grep -o -m1 "Negative number of records in the NetCDF header"
rm -f "$FILE"

echo "--- the streaming marker is still accepted"
write_file "$FILE" 0xFFFFFFFF 3
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 0"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1"
rm -f "$FILE"

echo "--- and so is a concrete count"
write_file "$FILE" 3 3
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) ORDER BY ALL"
rm -f "$FILE"
