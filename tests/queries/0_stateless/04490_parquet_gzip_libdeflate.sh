#!/usr/bin/env bash
# Tags: no-fasttest
# Roundtrip varied data through Parquet with the gzip codec (compressed and decompressed by
# libdeflate when available) and verify the data is byte-for-byte identical across levels.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

# A column mix that is partly compressible (repeating strings, sequential ints) and partly
# incompressible (random), with nullable values.
gen="SELECT
        number AS id,
        if(number % 7 = 0, NULL, toString(number % 1000)) AS cat,
        concat('user_', toString(cityHash64(number) % 100000)) AS name,
        reinterpretAsString(cityHash64(number, 'salt')) AS blob,
        number / 3 AS val
     FROM numbers(100000)"

reference=$($CLICKHOUSE_LOCAL -q "$gen ORDER BY id" | md5sum)

# Levels are kept within zlib's [1, 9] range so the test also passes in the
# ENABLE_LIBDEFLATE=0 fallback build (where Parquet gzip falls back to zlib,
# which rejects levels above 9). In the default build these still go through
# libdeflate; its extended levels 10-12 are covered by the gtest_libdeflate* tests.
for level in 1 3 6 9; do
    roundtrip=$($CLICKHOUSE_LOCAL -q "$gen FORMAT Parquet
            SETTINGS output_format_parquet_compression_method='gzip', output_format_compression_level=$level" \
        | $CLICKHOUSE_LOCAL --input-format=Parquet -q "SELECT * FROM table ORDER BY id" \
        | md5sum)
    if [ "$roundtrip" = "$reference" ]; then
        echo "level $level: OK"
    else
        echo "level $level: MISMATCH ($roundtrip vs $reference)"
    fi
done
