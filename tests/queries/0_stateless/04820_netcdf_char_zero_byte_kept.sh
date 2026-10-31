#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}

# A `char` variable whose dimensions all stay in the row space holds one character per row, not
# padded strings, so a zero byte there is data and must be kept. Only the strings that a stripped
# length dimension holds are padded with zero bytes, so only they are trimmed. Two files: in the
# first the dimension of the characters has a variable of its own (`char station(station)`), so a
# `\0` element must survive as a one-byte string; in the second nothing else uses the last
# dimension, so it is the length of the strings and the padding is still removed.
python3 - "$PREFIX" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_CHAR = 10, 11, 2
ABSENT = struct.pack('>ii', 0, 0)


def tag(value):
    return struct.pack('>i', value)


def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)


def pad4(data):
    return data + b'\x00' * ((4 - len(data) % 4) % 4)


def build(dimensions, variables):
    """dimensions: [(name, length)]; variables: [(name, [dimension ids], type, data)]"""
    def header(begins):
        result = b'CDF\x01' + tag(0)
        result += tag(NC_DIMENSION) + tag(len(dimensions))
        for dimension_name, length in dimensions:
            result += name(dimension_name) + tag(length)
        result += ABSENT
        result += tag(NC_VARIABLE) + tag(len(variables))
        for (variable_name, dimension_ids, variable_type, data), begin in zip(variables, begins):
            result += name(variable_name) + tag(len(dimension_ids))
            for dimension_id in dimension_ids:
                result += tag(dimension_id)
            result += ABSENT + tag(variable_type) + tag(len(pad4(data))) + tag(begin)
        return result

    begins = []
    offset = len(header([0] * len(variables)))
    for _, _, _, data in variables:
        begins.append(offset)
        offset += len(pad4(data))

    return header(begins) + b''.join(pad4(data) for _, _, _, data in variables)


prefix = sys.argv[1]

# `char station(station)`: one character per row, the middle one is a zero byte.
with open(prefix + '_row.nc', 'wb') as out:
    out.write(build([('station', 3)], [('station', [0], NC_CHAR, b'a\x00c')]))

# `char nm(station, nchar)`: `nchar` is the length of the strings, the padding is trimmed.
with open(prefix + '_padded.nc', 'wb') as out:
    out.write(build(
        [('station', 2), ('nchar', 3)],
        [('nm', [0, 1], NC_CHAR, b'ab\x00cd\x00')]))
PYTHON

echo "--- row"
$CLICKHOUSE_LOCAL -q "SELECT hex(station) FROM file('${PREFIX}_row.nc', NetCDF)"
echo "--- padded"
$CLICKHOUSE_LOCAL -q "SELECT hex(nm) FROM file('${PREFIX}_padded.nc', NetCDF)"

rm -f "${PREFIX}_row.nc" "${PREFIX}_padded.nc"
