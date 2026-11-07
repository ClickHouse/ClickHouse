#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A `char label(nchar)` of 16 KiB is one string broadcast over every row of `int v(x)` of 65536 rows.
# A chunk of the default number of rows would materialize about 1 GiB of copies of the label before
# `LIMIT 1` could stop the query, so the rows of a chunk are bounded by its size in bytes.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_CHAR, NC_INT = 10, 11, 2, 4
ABSENT = struct.pack('>ii', 0, 0)
NCHAR, X = 16384, 65536

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

def make_header(label_begin, v_begin):
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(2) + name('nchar') + tag(NCHAR) + name('x') + tag(X) + ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('label') + tag(1) + tag(0) + ABSENT + tag(NC_CHAR) + tag(NCHAR) + tag(label_begin)
    header += name('v') + tag(1) + tag(1) + ABSENT + tag(NC_INT) + tag(4 * X) + tag(v_begin)
    return header

size = len(make_header(0, 0))
with open(sys.argv[1], 'wb') as out:
    out.write(make_header(size, size + NCHAR))
    out.write(b'L' * NCHAR)
    out.write(b''.join(struct.pack('>i', i) for i in range(X)))
PYTHON

$CLICKHOUSE_LOCAL -q "SELECT length(label), v FROM file('$FILE', NetCDF) LIMIT 1 SETTINGS max_memory_usage = '200Mi', max_threads = 1"
$CLICKHOUSE_LOCAL -q "SELECT count(), uniqExact(label), sum(v) FROM file('$FILE', NetCDF) SETTINGS max_memory_usage = '200Mi', max_threads = 1"

# `input_format_max_block_size_bytes` makes the chunks smaller still.
$CLICKHOUSE_LOCAL -q "SELECT max(blockSize()) FROM file('$FILE', NetCDF) SETTINGS input_format_max_block_size_bytes = 100000, max_threads = 1"

rm -f "$FILE"
