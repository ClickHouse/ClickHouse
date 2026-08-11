#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to craft the fixtures, and Parquet is not built in fasttest.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DATA"
mkdir -p "$DATA"
trap 'rm -rf "$DATA"' EXIT

# A DECIMAL whose declared precision cannot fit its physical element (INT32 caps at 9 digits, INT64
# at 18) is spec-violating. The reader must reject it, not decode into a too-narrow column.
python3 - "$DATA" <<'PYEOF'
import decimal, os, struct, sys
import pyarrow as pa
import pyarrow.parquet as pq

OUT = sys.argv[1]
SCALE2 = bytes([0x15, 0x04])
PREC = {9: bytes([0x15, 0x12]), 18: bytes([0x15, 0x24]), 38: bytes([0x15, 0x4C])}


def patch_precision(path, was, now):
    b = bytearray(open(path, "rb").read())
    assert b[:4] == b"PAR1" and b[-4:] == b"PAR1", "not a parquet file"
    flen = struct.unpack("<I", b[-8:-4])[0]
    start = len(b) - 8 - flen
    foot = b[start:start + flen]
    assert foot.count(SCALE2 + PREC[was]) >= 1, f"declared precision {was} absent"
    patched = foot.replace(SCALE2 + PREC[was], SCALE2 + PREC[now])
    # Prove the widen actually landed: the old precision varint is gone and the new one is present,
    # so the footer now declares Decimal(now, 2) over the same, untouched physical element.
    assert SCALE2 + PREC[was] not in patched and patched.count(SCALE2 + PREC[now]) >= 1, \
        f"precision {was} -> {now} rewrite did not take"
    b[start:start + flen] = patched
    open(path, "wb").write(bytes(b))


def write(name, physical_precision, declare, physical):
    path = os.path.join(OUT, name + ".parquet")
    vals = [decimal.Decimal(f"{i}.{i % 100:02d}") for i in range(20)]
    pq.write_table(pa.table({"k": pa.array(vals, type=pa.decimal128(physical_precision, 2))}),
                   path, use_dictionary=True, compression="none", version="2.6",
                   store_decimal_as_integer=True, write_statistics=True)
    # Prove the physical layout is the narrow integer type the guard keys off, while the file is
    # still spec-valid. This must run pre-patch: once patch_precision widens the declared precision
    # past what INT32/INT64 can hold, pyarrow itself refuses to open the file (the very spec
    # violation the guard rejects), so a post-patch pq.ParquetFile read is impossible here.
    c = pq.ParquetFile(path).schema.column(0)
    assert c.physical_type == physical, f"{name}: physical {c.physical_type}, wanted {physical}"
    assert str(c.logical_type) == f"Decimal(precision={physical_precision}, scale=2)", \
        f"{name}: pre-patch logical {c.logical_type}"
    patch_precision(path, physical_precision, declare)


# INT32 physical (max 9 digits), declared precision 18 -> 18 > 9.
write("int32_prec18", 9, 18, "INT32")
# INT64 physical (max 18 digits), declared precision 38 -> 38 > 18.
write("int64_prec38", 18, 38, "INT64")
PYEOF

echo '-- INT32 physical with declared precision 18 (exceeds 9): rejected'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/int32_prec18.parquet', Parquet)" 2>&1 | grep -o -m1 'precision or scale is too big'

echo '-- INT64 physical with declared precision 38 (exceeds 18): rejected'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/int64_prec38.parquet', Parquet)" 2>&1 | grep -o -m1 'precision or scale is too big'
