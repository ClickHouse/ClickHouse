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

# Every `clickhouse-local` of this test starts a new process, and a start takes tens of seconds
# under a sanitizer, so the cases are read in parallel and their output is printed afterwards in
# the order of the cases.
read_file()
{
    local case_name=$1
    local dimension_name=$2
    local variable_name=$3

    write_file "${FILE}.${case_name}" "$dimension_name" "$variable_name"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('${FILE}.${case_name}', NetCDF)" > "${FILE}.${case_name}.out" 2>&1 &
}

read_file conforming 74 76
read_file empty_variable_name 74 ''
read_file not_utf8 74 76ff
read_file control_character 74 760178
read_file trailing_space 74 7620
read_file leading_dot 2e74 76
read_file slash 612f62 76

wait

echo "--- a conforming file is read"
cat "${FILE}.conforming.out"

for case_name in empty_variable_name not_utf8 control_character trailing_space leading_dot slash
do
    case "$case_name" in
        empty_variable_name) echo "--- an empty variable name" ;;
        not_utf8) echo "--- a variable name that is not valid UTF-8" ;;
        control_character) echo "--- a variable name with a control character" ;;
        trailing_space) echo "--- a variable name with a trailing space" ;;
        leading_dot) echo "--- a dimension name that begins with a dot" ;;
        slash) echo "--- a dimension name with a slash" ;;
    esac
    grep -c "INCORRECT_DATA" "${FILE}.${case_name}.out"
done

rm -f "${FILE}".*
