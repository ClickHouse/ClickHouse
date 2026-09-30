#!/usr/bin/env bash

# Regression test for issue #104831: subnormal float16 values were converted to
# Float32 with an off-by-one mantissa shift. The fixture contains float16 bits
# 0x0001, 0x0200, 0x0202, 0x03FF, 0x83FF, 0x0000 — exercising the smallest
# positive subnormal, a power-of-two subnormal (worked before the fix), a
# non-power-of-two subnormal, the largest +/- subnormals, and zero.
#
# Regenerate with:
#   python3 -c "import numpy as np; \
#     b = np.array([0x0001,0x0200,0x0202,0x03FF,0x83FF,0x0000], dtype=np.uint16); \
#     np.save('float_16_subnormals.npy', b.view(np.float16))"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CUR_DIR/data_npy/float_16_subnormals.npy')"
