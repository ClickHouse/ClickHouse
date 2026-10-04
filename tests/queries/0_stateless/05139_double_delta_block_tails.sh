#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

python3 - <<'PYTHON'
import os
import random
import shlex
import subprocess

compressor = shlex.split(os.environ["CLICKHOUSE_COMPRESSOR"])
rng = random.Random(20260908)

def roundtrip(data, width, block_size=65536):
    encoded = subprocess.check_output(compressor + ["--codec", f"DoubleDelta({width})", "--block-size", str(block_size)], input=data)
    decoded = subprocess.check_output(compressor + ["--decompress"], input=encoded)
    assert decoded == data, (width, block_size, len(data))

for width in (1, 2, 4, 8):
    for length in (0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 4096, 65536):
        roundtrip(bytes(rng.getrandbits(8) for _ in range(length)), width)

    mask = (1 << (8 * width)) - 1
    deltas = [0]
    for boundary in (1, 63, 64, 255, 256, 2047, 2048, (1 << 31) - 1, 1 << 31, (1 << 63) - 1):
        deltas.extend((boundary, -boundary))
    values = []
    value = delta = 0
    for difference in deltas * 5:
        delta = (delta + difference) & mask
        value = (value + delta) & mask
        values.append(value.to_bytes(width, "little"))
    for block_size in (17, 31, 64, 4096, 65536):
        roundtrip(b"".join(values), width, block_size)
    print(f"width {width}: OK")
PYTHON
