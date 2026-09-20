#!/usr/bin/env bash

# Npy reports the number of rows its header declares, or fails. A shape that
# over-declares the data is rejected on the count paths too, not only when the
# data is read with a knowable size (issue #99585, test 04056).

# Every assertion runs in its own clickhouse-local process: the row-count cache
# lives in the process, so a real read in one assertion cannot answer another.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DATA_DIR"
trap 'rm -rf "${DATA_DIR:?}"' EXIT

# All fixtures hold exactly 10 <i8 rows (80 bytes); the shape says otherwise.
python3 -c "
import gzip, struct, sys
import numpy as np

d = sys.argv[1]

def npy(descr, shape, body):
    header = (\"{'descr': '%s', 'fortran_order': False, 'shape': (%s), }\" % (descr, shape)).encode()
    pad_len = 64 - (6 + 2 + 2 + len(header)) % 64
    if pad_len < 1: pad_len += 64
    header += b' ' * (pad_len - 1) + b'\n'
    return b'\x93NUMPY' + b'\x01\x00' + struct.pack('<H', len(header)) + header + body

def write(name, data):
    opener = gzip.open if name.endswith('.gz') else open
    with opener(d + '/' + name, 'wb') as f:
        f.write(data)

ten_rows = b''.join(struct.pack('<q', i) for i in range(10))
over = npy('<i8', '1000000000,', ten_rows)

write('huge.npy', over)
write('huge.npy.gz', over)
# 10 rows plus one byte of the 11th: the read path already rejects this, count() did not.
write('partial.npy.gz', npy('<i8', '1000000000,', ten_rows + b'\x01'))
write('short.npy', npy('<i8', '12,', ten_rows))
write('ok.npy', npy('<i8', '10,', ten_rows))
write('ok.npy.gz', npy('<i8', '10,', ten_rows))

np.save(d + '/s0.npy', np.ndarray(shape=(3,), dtype='|S0'))
with open(d + '/s0.npy', 'rb') as f:
    write('s0.npy.gz', f.read())
" "$DATA_DIR"

# The row-count cache discards an entry whose file is not strictly older than it
# (SchemaCache::tryGetImpl, whole seconds), so a just-written fixture would never
# be answered from the cache at all.
touch -t 202001010000 "$DATA_DIR"/*.npy "$DATA_DIR"/*.npy.gz

# Exactly one line per assertion: the count, or the error code it was rejected with.
verdict() {
    local out
    out=$(grep -m1 -oE 'INCORRECT_DATA|CANNOT_READ_ALL_DATA|^[0-9]+$')
    echo "${out:-UNEXPECTED}"
}

# 1-3: every count path must agree with the read path, which rejects this file.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/huge.npy', 'Npy') SETTINGS optimize_count_from_files = 1" 2>&1 | verdict
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/huge.npy', 'Npy') SETTINGS optimize_count_from_files = 0" 2>&1 | verdict
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/huge.npy', 'Npy') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 0" 2>&1 | verdict

# 4-5: the inferred schema is unchanged, but the unvalidated row count is no longer cached.
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('$DATA_DIR/huge.npy', 'Npy')" | cut -f 1,2
${CLICKHOUSE_LOCAL} -m --query "
DESCRIBE file('$DATA_DIR/huge.npy', 'Npy');
SELECT number_of_rows IS NULL FROM system.schema_inference_cache WHERE format = 'Npy';
" | tail -n 1

# 6-10: compressed input has no knowable size, so the rows themselves must vouch for the count.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM (SELECT * FROM file('$DATA_DIR/huge.npy.gz', 'Npy'))" 2>&1 | verdict
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/huge.npy.gz', 'Npy') SETTINGS optimize_count_from_files = 0" 2>&1 | verdict
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/huge.npy.gz', 'Npy') SETTINGS optimize_count_from_files = 1" 2>&1 | verdict
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/huge.npy.gz', 'Npy', 'array Int64') SETTINGS optimize_count_from_files = 1" 2>&1 | verdict
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/partial.npy.gz', 'Npy')" 2>&1 | verdict

# 11: over-declaring by two rows is rejected just like over-declaring by a billion.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('$DATA_DIR/short.npy', 'Npy')" 2>&1 | verdict

# 12: a file that delivers what it declares still counts, on every path.
# 13: a zero-byte payload cannot contradict any shape, so the header still answers.
# These all succeed and read different files, so one process cannot skew another.
${CLICKHOUSE_LOCAL} -m --query "
SELECT count() FROM file('$DATA_DIR/ok.npy', 'Npy') SETTINGS optimize_count_from_files = 1;
SELECT count() FROM file('$DATA_DIR/ok.npy', 'Npy') SETTINGS optimize_count_from_files = 0;
SELECT count() FROM file('$DATA_DIR/ok.npy', 'Npy', 'array Int64');
SELECT count() FROM file('$DATA_DIR/ok.npy.gz', 'Npy');
SELECT count() FROM file('$DATA_DIR/s0.npy.gz', 'Npy');
"

# A confirmed header answers count() without reading the 8 MB payload. Pinned because the
# runner disables optimize_count_from_files with probability 0.05, which would make this a scan.
${CLICKHOUSE_LOCAL} -m --query "
SELECT count() FROM file('$CURDIR/data_npy/npy_big.npy', 'Npy') SETTINGS optimize_count_from_files = 1;
SELECT sum(value) < 1000000 AND count() = 1 FROM system.events WHERE event = 'SelectedBytes';
"

# The row-count cache a bare DESCRIBE no longer pre-fills is still filled by a real read.
${CLICKHOUSE_LOCAL} -m --query "
SELECT count() FROM file('$DATA_DIR/ok.npy', 'Npy') SETTINGS optimize_count_from_files = 0;
SELECT number_of_rows FROM system.schema_inference_cache WHERE format = 'Npy';
"

# An early stop is served from the rows that are there: where the size is not knowable, the
# declared count is contradicted only once a consumer asks for a row past the data.
# max_block_size is pinned (the runner randomizes it) so that the stop is one row at a time.
${CLICKHOUSE_LOCAL} -m --query "
SELECT count() FROM (SELECT * FROM file('$DATA_DIR/huge.npy.gz', 'Npy') LIMIT 1 SETTINGS max_block_size = 1);
SELECT count() FROM (SELECT * FROM file('$DATA_DIR/huge.npy.gz', 'Npy') LIMIT 11 SETTINGS max_block_size = 1);
" 2>&1 | grep -oE 'CANNOT_READ_ALL_DATA|^[0-9]+$'
