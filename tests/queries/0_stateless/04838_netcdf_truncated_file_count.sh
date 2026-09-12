#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The header of a NetCDF file carries the lengths of the dimensions and the offset of the data of
# every variable, without any relation to the size of the file, so a truncated file still has a
# well-formed header that promises rows the file does not contain. The number of rows of such a
# file must not be answered from its header: it goes into the schema cache and would let
# `optimize_count_from_files` return a count for a file that the reader rejects.
#
# The file below is a CDF-1 file with the dimension `x` of the length 3 and the variable `int v(x)`.
write_file()
{
    python3 - "$1" "$2" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

def header(begin_of_v):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1)
    result += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(12) + tag(begin_of_v)
    return result

size = len(header(0))
data = header(size) + struct.pack('>iii', 1, 2, 3)

# The number of values of `v` that the file actually carries.
with open(sys.argv[1], 'wb') as out:
    out.write(data[:size + 4 * int(sys.argv[2])])
PYTHON
    # The schema cache treats an entry as stale when the file was modified at or after the time the
    # entry was registered, and both happen in the same second here, so the file has to be backdated
    # for the cache to be used at all.
    touch -d '2020-01-01 00:00:00' "$1"
}

echo "--- the whole file"
write_file "$FILE" 3
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 0"

echo "--- truncated after the header"
write_file "$FILE" 0
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1" 2>&1 |
    grep -o -m1 "The data of the variable v does not fit in the NetCDF file: it needs 92 bytes, but the file is 80 bytes"

echo "--- truncated in the middle of the data"
write_file "$FILE" 2
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1" 2>&1 |
    grep -o -m1 "The data of the variable v does not fit in the NetCDF file: it needs 92 bytes, but the file is 88 bytes"

echo "--- and the count of a truncated file is not served from the cache of the whole file"
# The two files have the same name, so the entry of the first one is in the cache when the second
# one is read. `SELECT count()` of the first file is answered from its header, and the count of the
# truncated file must not be answered from that entry.
write_file "$FILE" 3
$CLICKHOUSE_LOCAL -m -q "
SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1;
" > /dev/null
write_file "$FILE" 0
$CLICKHOUSE_LOCAL -m -q "
SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1;
" 2>&1 | grep -o -m1 "does not fit in the NetCDF file"

rm -f "$FILE"
