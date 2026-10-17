#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The sizes and counts of the header (`NON_NEG` in the specification) are unsigned, as the
# reference implementation decodes them: the high bit of a dimension length or of the number of
# records is not a sign bit. Only the all-ones number of records is reserved as the streaming marker.

# One unlimited dimension `t` and one record variable `int v(t)`. The number of records is the
# argument. The header of CDF-2 stores sizes as 32 bits and offsets as 64 bits, so it is 84 bytes.
write_records_file()
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

# An unused dimension `big` of the given length and a scalar `int answer = 42`, in CDF-2 or CDF-5.
write_big_dimension_file()
{
    python3 - "$1" "$2" "$3" <<'PYTHON'
import struct
import sys

version = int(sys.argv[2])
size_format = '>Q' if version == 5 else '>I'

def tag(value):
    return struct.pack('>i', value)

def size(value):
    return struct.pack(size_format, value)

def name(value):
    data = value.encode()
    return size(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + size(0)

header = b'CDF' + bytes([version]) + size(0)
header += tag(NC_DIMENSION) + size(1) + name('big') + size(int(sys.argv[3], 0))
header += ABSENT
header += tag(NC_VARIABLE) + size(1)
header += name('answer') + size(0) + ABSENT + tag(NC_INT) + size(4)
begin = len(header) + 8
header += struct.pack('>q', begin)

with open(sys.argv[1], 'wb') as out:
    out.write(header + struct.pack('>i', 42))
PYTHON
}

echo "--- 0xFFFFFFFE records in the header of a CDF-2 file is a legal count, but the file does not have them"
write_records_file "$FILE" 0xFFFFFFFE
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF)" 2>&1 |
    grep -o -m1 "The data of the variable v does not fit in the NetCDF file"
rm -f "$FILE"

echo "--- and so is 0x80000000"
write_records_file "$FILE" 0x80000000
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 |
    grep -o -m1 "The data of the variable v does not fit in the NetCDF file"
rm -f "$FILE"

echo "--- the streaming marker is still accepted"
write_records_file "$FILE" 0xFFFFFFFF 3
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 0"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1"
rm -f "$FILE"

echo "--- and so is a concrete count"
write_records_file "$FILE" 3 3
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF) ORDER BY ALL"
rm -f "$FILE"

echo "--- an unused dimension of 2^31 elements in CDF-2"
write_big_dimension_file "$FILE" 2 0x80000000
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"

echo "--- an unused dimension of 2^63 elements in CDF-5"
write_big_dimension_file "$FILE" 5 0x8000000000000000
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"
