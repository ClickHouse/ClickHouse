#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The names of the dimensions and of the variables of a file become the names of the columns, so a
# malformed file must not be able to publish an arbitrary byte string as a column name. The names
# have to follow the same rules of the classic format that the writer enforces.

write_file()
{
    python3 - "$1" "$2" "$3" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    return tag(len(value)) + value + b'\x00' * ((4 - len(value) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

# The names are passed as hexadecimal, so that a name that is not valid UTF-8 survives the shell.
path, dimension_name, variable_name = sys.argv[1], bytes.fromhex(sys.argv[2]), bytes.fromhex(sys.argv[3])

# One fixed dimension of two elements and one variable over it, with four bytes per element.
header = b'CDF\x01' + tag(0)
header += tag(NC_DIMENSION) + tag(1) + name(dimension_name) + tag(2)
header += ABSENT
header += tag(NC_VARIABLE) + tag(1)
prefix = header + name(variable_name) + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(8)
begin = len(prefix) + 4
data = prefix + tag(begin) + tag(1) + tag(2)
assert len(data) == begin + 8, (len(data), begin)

with open(path, 'wb') as f:
    f.write(data)
PYTHON
}

read_file()
{
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)" 2>&1 | grep -c "$1"
}

echo "--- a conforming file is read"
write_file "$FILE" 74 76
read_file 'INCORRECT_DATA'
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

echo "--- an empty variable name"
write_file "$FILE" 74 ''
read_file 'INCORRECT_DATA'

echo "--- a variable name that is not valid UTF-8"
write_file "$FILE" 74 76ff
read_file 'INCORRECT_DATA'

echo "--- a variable name with a control character"
write_file "$FILE" 74 760178
read_file 'INCORRECT_DATA'

echo "--- a variable name with a trailing space"
write_file "$FILE" 74 7620
read_file 'INCORRECT_DATA'

echo "--- a dimension name that begins with a dot"
write_file "$FILE" 2e74 76
read_file 'INCORRECT_DATA'

echo "--- a dimension name with a slash"
write_file "$FILE" 612f62 76
read_file 'INCORRECT_DATA'

rm -f "$FILE"
