#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A floating-point `_FillValue` or `missing_value` is compared by value, as the reference
# implementation does it: `-0.0` matches the sentinel `+0.0`, and a NaN sentinel matches every NaN,
# whatever its payload. Every variable holds four values, two of which match its sentinel.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_ATTRIBUTE = 10, 11, 12
NC_FLOAT, NC_DOUBLE = 5, 6
ABSENT = struct.pack('>ii', 0, 0)

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

def attribute(name_, type_, value):
    return name(name_) + tag(type_) + tag(1) + value

F32_ZERO, F32_NEG_ZERO = bytes.fromhex('00000000'), bytes.fromhex('80000000')
F32_NAN, F32_OTHER_NAN = bytes.fromhex('7fc00000'), bytes.fromhex('7fc00001')
F64_ZERO, F64_NEG_ZERO = bytes.fromhex('0000000000000000'), bytes.fromhex('8000000000000000')
F64_NAN, F64_OTHER_NAN = bytes.fromhex('7ff8000000000000'), bytes.fromhex('fff8000000000001')

variables = [
    ('f32_fill_zero', NC_FLOAT, '_FillValue', F32_ZERO, [F32_NEG_ZERO, F32_ZERO, struct.pack('>f', 1), F32_NAN]),
    ('f32_missing_nan', NC_FLOAT, 'missing_value', F32_NAN, [F32_OTHER_NAN, F32_NAN, struct.pack('>f', 1), F32_NEG_ZERO]),
    ('f64_fill_nan', NC_DOUBLE, '_FillValue', F64_NAN, [F64_OTHER_NAN, F64_NAN, struct.pack('>d', 1), F64_NEG_ZERO]),
    ('f64_missing_zero', NC_DOUBLE, 'missing_value', F64_ZERO, [F64_NEG_ZERO, F64_ZERO, struct.pack('>d', 1), F64_NAN]),
]

def make_header(begins):
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(4) + ABSENT
    header += tag(NC_VARIABLE) + tag(len(variables))
    for (var_name, type_, attribute_name, sentinel, values), begin in zip(variables, begins):
        header += name(var_name) + tag(1) + tag(0)
        header += tag(NC_ATTRIBUTE) + tag(1) + attribute(attribute_name, type_, sentinel)
        header += tag(type_) + tag(len(b''.join(values))) + tag(begin)
    return header

size = len(make_header([0] * len(variables)))
begins = []
for variable in variables:
    begins.append(size)
    size += len(b''.join(variable[4]))

with open(sys.argv[1], 'wb') as out:
    out.write(make_header(begins))
    for variable in variables:
        out.write(b''.join(variable[4]))
PYTHON

for column in f32_fill_zero f32_missing_nan f64_fill_nan f64_missing_zero
do
    $CLICKHOUSE_LOCAL -q "SELECT '$column', groupArray(isNull($column)) FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"
done

rm -f "$FILE"
