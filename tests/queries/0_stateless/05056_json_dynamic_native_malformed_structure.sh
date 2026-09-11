#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Malformed structure prefixes of JSON/Dynamic columns in a Native block. Each of them used to be
# accepted and then read out of bounds, or to escape as an internal error instead of a data error.

DATA_DIR=$CLICKHOUSE_TMP/native_structure_$CLICKHOUSE_DATABASE
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

python3 - "$DATA_DIR" <<'EOF'
import struct
import sys

data_dir = sys.argv[1]

def s(b):
    return bytes([len(b)]) + b

def block(column_type, tail, rows=2):
    """One Native block with a single column and a hand-built serialization prefix."""
    return bytes([1, rows]) + s(b'j') + s(column_type) + tail

def varuint(n):
    out = b''
    while True:
        byte = n & 0x7f
        n >>= 7
        out += bytes([byte | 0x80]) if n else bytes([byte])
        if not n:
            return out

def object_prefix(version, paths):
    return struct.pack('<Q', version) + varuint(len(paths)) + b''.join(s(p) for p in paths)

def dynamic_prefix(types):
    return struct.pack('<Q', 3) + bytes([len(types)]) + b''.join(s(t) for t in types)

payloads = {
    # V3 of both types is written only into MergeTree parts, it opens the whole shared data
    # machinery (buckets, granules, marks) to a hand-built block.
    'object_v3': block(b'JSON', struct.pack('<Q', 4)),
    'dynamic_v3': block(b'Dynamic', struct.pack('<Q', 4)),
    # Two equal types map onto one variant discriminator.
    'dynamic_duplicate_types': block(b'Dynamic', dynamic_prefix([b'Int64', b'Int64'])),
    # Nothing is dropped by Variant, so it has no discriminator to unflatten into.
    'dynamic_nothing_type': block(b'Dynamic', dynamic_prefix([b'Nothing'])),
    # The same path is added both as a dynamic path and into shared data.
    'object_duplicate_flattened_path': block(b'JSON', object_prefix(3, [b'x', b'x'])),
    'object_duplicate_dynamic_path': block(b'JSON', object_prefix(2, [b'x', b'x'])),
    # A path that the type of the column already declares as a typed path.
    'object_typed_path_collision_flattened': block(b'JSON(x Int64)', object_prefix(3, [b'x'])),
    'object_typed_path_collision': block(b'JSON(x Int64)', object_prefix(2, [b'x'])),
    # More paths than the reader pre-allocates (1000000), so the list grows while it is read and the
    # duplicate has to be found without holding views into the buffer that was reallocated away.
    'object_duplicate_path_after_realloc': block(
        b'JSON(max_dynamic_paths=0)',
        object_prefix(3, [('p%d' % i).encode() for i in range(1000049)] + [b'p0']),
        rows=1),
}

for name, payload in payloads.items():
    with open(f'{data_dir}/{name}.bin', 'wb') as f:
        f.write(payload)
EOF

for name in object_v3 dynamic_v3 dynamic_duplicate_types dynamic_nothing_type \
            object_duplicate_flattened_path object_duplicate_dynamic_path \
            object_typed_path_collision_flattened object_typed_path_collision \
            object_duplicate_path_after_realloc
do
    echo -n "$name: "
    # Print the innermost exception: the outer one comes from schema inference and names the file.
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/$name.bin', Native)" 2>&1 \
        | grep -oE "DB::Exception: [^(]*\((INCORRECT_DATA|LOGICAL_ERROR)\)" | tail -1 \
        | sed -E 's/DB::Exception: //'
done

rm -rf "$DATA_DIR"
