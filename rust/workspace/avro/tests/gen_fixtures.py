#!/usr/bin/env python3
"""Builds the fixtures that the Rust tests read from AVRO_FIXTURES_DIR
(default: rust/workspace/avro/target/avro-fixtures).

For every `.avro` file in the repository (tests/queries/0_stateless and
tests/integration) it writes a copy whose blocks are decompressed and whose
`avro.codec` is `null`, so the Rust tests never need a decompressor, and a
manifest with what `fastavro` (an independent implementation) makes of the
original file: whether it reads it, and how many records it finds.

Usage: gen_fixtures.py <repo root> <out dir>
Requires: fastavro (with its optional snappy and zstandard dependencies).
"""
import json
import os
import sys

import fastavro
import fastavro.schema


def zz(v):
    n = (v << 1) ^ (v >> 63)
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def avro_bytes(b):
    return zz(len(b)) + b


def find_avro(root):
    for base in ("tests/queries/0_stateless", "tests/integration"):
        for d, _dirs, files in os.walk(os.path.join(root, base)):
            for f in sorted(files):
                if f.endswith(".avro"):
                    yield os.path.join(d, f)


def decompress_copy(path):
    """Returns (codec, null-codec file bytes) or raises."""
    with open(path, "rb") as f:
        r = fastavro.block_reader(f)
        header = r._header
        codec = r.codec
        blocks = [(b.num_records, b.bytes_.getvalue()) for b in r]
    meta = dict(header["meta"])
    meta["avro.codec"] = b"null"
    out = bytearray(b"Obj\x01")
    out += zz(len(meta))
    for k, v in meta.items():
        out += avro_bytes(k.encode())
        out += avro_bytes(v)
    out += zz(0)
    sync = header["sync"]
    out += sync
    for n, payload in blocks:
        out += zz(n) + zz(len(payload)) + payload + sync
    return codec, bytes(out)


def canonical_of(path):
    """(Parsing Canonical Form, fingerprint hex) of the writer schema, by fastavro."""
    with open(path, "rb") as f:
        r = fastavro.block_reader(f)
        schema_json = r._header["meta"]["avro.schema"]
    parsed = fastavro.schema.parse_schema(json.loads(schema_json))
    canon = fastavro.schema.to_parsing_canonical_form(parsed)
    return canon, fastavro.schema.fingerprint(canon, "CRC-64-AVRO")


def count_records(path):
    n = 0
    with open(path, "rb") as f:
        for _ in fastavro.reader(f):
            n += 1
    return n


def main():
    root, out = sys.argv[1], sys.argv[2]
    os.makedirs(os.path.join(out, "ocf"), exist_ok=True)
    rows = []
    for path in find_avro(root):
        rel = os.path.relpath(path, root)
        codec, data, n, status = "?", None, 0, "ok"
        try:
            codec, data = decompress_copy(path)
        except Exception as e:  # the oracle rejecting the file is data, not a failure
            status = "error: " + type(e).__name__ + ": " + str(e).replace("\t", " ").replace("\n", " ")[:120]
        if status == "ok":
            try:
                n = count_records(path)
            except Exception as e:
                status = "error: " + type(e).__name__ + ": " + str(e).replace("\t", " ").replace("\n", " ")[:120]
        target = os.path.join(out, "ocf", rel)
        if status == "ok":
            canon, fp = canonical_of(path)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target + ".canon", "w") as f:
                f.write(canon + "\n" + fp + "\n")
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if data is not None:
            with open(target, "wb") as f:
                f.write(data)
        elif os.path.exists(target):
            os.remove(target)
        rows.append((rel, codec, status, n))
    # Unscaled decimal values of decimals.avro, as computed by fastavro.
    os.makedirs(os.path.join(out, "values"), exist_ok=True)
    dec = os.path.join(root, "tests/queries/0_stateless/data_avro/decimals.avro")
    if os.path.exists(dec):
        with open(dec, "rb") as f, open(os.path.join(out, "values", "decimals.txt"), "w") as o:
            for rec in fastavro.reader(f):
                o.write(str(int(rec["d"].scaleb(4))) + "\n")
    with open(os.path.join(out, "ocf", "manifest.tsv"), "w") as f:
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")
    print(f"{len(rows)} files, {sum(1 for r in rows if r[2] == 'ok')} readable by fastavro")


if __name__ == "__main__":
    main()
