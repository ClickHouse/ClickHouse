#!/usr/bin/env bash
# Tags: no-fasttest
# Round-trip varied data through Parquet with the `gzip` codec at non-default compression levels and
# verify the data is identical. This keeps `output_format_compression_level` covered for
# `output_format_parquet_compression_method='gzip'` independently of the compression backend.

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

# Levels 10-12 are accepted for compatibility with 26.7 and clamped to zlib's maximum of 9.
for level in 1 3 6 9 12; do
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
