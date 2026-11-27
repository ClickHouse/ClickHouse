#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

obf="$CLICKHOUSE_FORMAT --obfuscate"

echo "CREATE TABLE t (a Int, b Int, INDEX idx_a a TYPE minmax, INDEX idx_b b TYPE set(3)) ENGINE = MergeTree ORDER BY tuple()" | $obf
echo "CREATE TABLE t (a String, INDEX idx1 a TYPE ngrambf_v1(3, 256, 2, 0), INDEX idx2 a TYPE tokenbf_v1(256, 2, 0), INDEX idx3 a TYPE bloom_filter(0.01)) ENGINE = MergeTree ORDER BY a" | $obf
echo "CREATE TABLE t (a Int CODEC(LZ4, Delta), b String CODEC(ZSTD(3)), c Float64 CODEC(DoubleDelta, LZ4HC)) ENGINE = MergeTree ORDER BY a" | $obf
