#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

echo "--- a name that is not valid UTF-8 cannot be written"
# The names of the classic format are UTF-8 text, and an identifier of ClickHouse is an arbitrary
# sequence of bytes, so a name with a byte that no UTF-8 sequence can contain, a dangling
# continuation byte or a truncated sequence has to be rejected instead of being written verbatim.
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`temp\xFF\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`\x80bad\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`temp\xD0\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- and a valid UTF-8 name outside of ASCII is written and read back"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`темп\` INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"

# A file written in the streaming mode does not store its number of records: it is derived from the
# size of the file, which the schema reader has to do as well, or the number of rows would not be
# available from the metadata.
write_file()
{
    python3 - "$1" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

# One unlimited dimension and one record variable of four bytes. The number of records is written
# as all ones, which is what a writer of the streaming mode puts there.
header = b'CDF\x01' + struct.pack('>I', 0xFFFFFFFF)
header += tag(NC_DIMENSION) + tag(1) + name('t') + tag(0)
header += ABSENT
header += tag(NC_VARIABLE) + tag(1)
header += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4) + tag(80)
assert len(header) == 80, len(header)

with open(sys.argv[1], 'wb') as out:
    out.write(header + b''.join(struct.pack('>i', value) for value in range(3)))
PYTHON
}

write_file "$FILE"

echo "--- the number of rows of a streaming file comes from the metadata"
$CLICKHOUSE_LOCAL -m -q "
DESCRIBE file('$FILE', NetCDF) FORMAT Null;
SELECT number_of_rows FROM system.schema_inference_cache WHERE format = 'NetCDF';
"

echo "--- and the count is the same with and without the fast path"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 0"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) SETTINGS optimize_count_from_files = 1"

rm -f "$FILE"
