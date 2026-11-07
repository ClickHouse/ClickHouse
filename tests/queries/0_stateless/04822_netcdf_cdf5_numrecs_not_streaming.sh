#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The streaming marker of the number of records is all ones of the width of the field, which is
# 64 bits wide in CDF-5, so the 32-bit marker of the older versions - 4294967295 - is a legal
# concrete number of records there and must not send the reader to the size-derived fallback.

# One unlimited dimension `t` and one record variable `int v(t)`. The number of records is the
# argument. The header of CDF-5 stores every size as 64 bits, so it is 128 bytes.
write_file()
{
    python3 - "$1" "$2" "${3:-0}" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def size(value):
    return struct.pack('>q', value)

def name(value):
    data = value.encode()
    return size(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + size(0)

header = b'CDF\x05' + struct.pack('>Q', int(sys.argv[2], 0))
header += tag(NC_DIMENSION) + size(1) + name('t') + size(0)
header += ABSENT
header += tag(NC_VARIABLE) + size(1)
header += name('v') + size(1) + size(0) + ABSENT + tag(NC_INT) + size(4) + size(128)
assert len(header) == 128, len(header)

num_data_records = int(sys.argv[3])
with open(sys.argv[1], 'wb') as out:
    out.write(header + b''.join(struct.pack('>i', value) for value in range(num_data_records)))
PYTHON
}

echo "--- 4294967295 records in the header of a CDF-5 file is a concrete count, not the streaming marker"
# The file does not carry the data of its 4294967295 records, so the concrete count is visible in
# the size that the reader demands of the file: (4294967295 - 1) * 4 + 128 + 4 = 17179869308 bytes.
# A reader that mistakes the stored value for the streaming marker would instead derive 0 records
# from the size of the file and read the file as an empty table.
write_file "$FILE" 0xFFFFFFFF
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF)" 2>&1 |
    grep -o -m1 "The data of the variable v does not fit in the NetCDF file: it needs 17179869308 bytes"
rm -f "$FILE"

echo "--- and the streaming marker of CDF-5 is the 64-bit all-ones value"
write_file "$FILE" 0xFFFFFFFFFFFFFFFF 3
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 0"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1"
rm -f "$FILE"
