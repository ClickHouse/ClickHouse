#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the native Arrow IPC reader is only built with the Arrow contrib.

# The native Arrow IPC reader chooses a branch on a FlatBuffers union discriminant
# (`Message.header_type`, `Field.type_type`) and then dereferences the matching typed
# accessor. FlatBuffers verification accepts a buffer whose discriminant is set while the
# union value offset is absent, so the accessor returns null. Dereferencing it used to
# crash the server (a remotely triggerable SIGSEGV reachable at schema-inference time on
# any `file()`/`url()`/`s3()` Arrow read). Both unions must now be rejected as INCORRECT_DATA.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# Hand-build the malformed Arrow IPC streams with pure struct packing (no pyarrow dependency),
# so the exact "discriminant set, value absent" wire shape is reproduced deterministically.
python3 - "$WORK_DIR" <<'PY'
import struct, sys, os
work = sys.argv[1]

def frame(meta):
    # Modern encapsulated-message framing + end-of-stream marker.
    return (struct.pack('<i', -1) + struct.pack('<i', len(meta)) + meta
            + struct.pack('<i', -1) + struct.pack('<i', 0))

# 1) Message whose header_type = Schema(1) but the header union value offset is omitted.
b = bytearray(32)
struct.pack_into('<I', b, 0, 20)                                  # root -> Message table @20
for o, v in [(4, 14), (6, 8), (8, 0), (10, 4), (12, 0), (14, 0), (16, 0)]:  # Message vtable, header slot(12)=0
    struct.pack_into('<H', b, o, v)
struct.pack_into('<i', b, 20, 16); b[24] = 1                      # soffset; header_type=Schema, header ABSENT
open(os.path.join(work, 'header.arrows'), 'wb').write(frame(bytes(b)))

# 2) Valid Schema with one Field whose type_type = Int(2) but the type union value offset is omitted.
b = bytearray(88)
def p16(o, v): struct.pack_into('<H', b, o, v)
def p32(o, v): struct.pack_into('<I', b, o, v)
def pi32(o, v): struct.pack_into('<i', b, o, v)
p32(0, 20)                                                        # root -> Message table @20
p16(4, 14); p16(6, 12); p16(8, 0); p16(10, 4); p16(12, 8); p16(14, 0); p16(16, 0)  # Message vtable
pi32(20, 16); b[24] = 1; p32(28, 16)                             # Message table: header_type=Schema, header ->Schema
p16(32, 12); p16(34, 8); p16(36, 0); p16(38, 4); p16(40, 0); p16(42, 0)            # Schema vtable
pi32(44, 12); p32(48, 4)                                          # Schema table: fields ->vector
p32(52, 1); p32(56, 24)                                           # fields vector: 1 element ->Field @80
p16(60, 18); p16(62, 8); p16(64, 0); p16(66, 0); p16(68, 4); p16(70, 0); p16(72, 0); p16(74, 0); p16(76, 0)  # Field vtable, type slot(70)=0
pi32(80, 20); b[84] = 2                                           # Field table: type_type=Int, type ABSENT
open(os.path.join(work, 'field.arrows'), 'wb').write(frame(bytes(b)))
PY

# Schema inference must reject both as corrupt data instead of crashing.
for name in header field; do
    ${CLICKHOUSE_LOCAL} -q "DESC file('$WORK_DIR/$name.arrows', 'ArrowStream')" 2>&1 \
        | grep -qF "type is set but its value is missing" && echo "$name: rejected" || echo "$name: NOT REJECTED"
done
