#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_long_record.parquet"
trap 'rm -f "$FILE"' EXIT

# A record with more values than the Parquet writer hands to a converter at once is converted in
# slices, which still go into the same page. Rows on both sides of the long one check that the slices
# keep the values and the row boundaries in place.
$CLICKHOUSE_LOCAL --query "
    SELECT
        number AS n,
        arrayMap(i -> toFixedString(toString(i % 10), 1), range(n = 1 ? 300000 : 3)) AS f,
        arrayMap(i -> toString(i % 7), range(n = 1 ? 300000 : 3)) AS s,
        arrayMap(i -> if(i % 5 = 0, NULL, toInt8(i % 100)), range(n = 1 ? 300000 : 3)) AS i,
        [arrayMap(i -> toFixedString(toString(i % 3), 1), range(n = 1 ? 300000 : 3))] AS ff
    FROM numbers(3)
    SETTINGS output_format_parquet_parallel_encoding = 0
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --query "
    SELECT
        n,
        length(f), arraySum(x -> toUInt8(x), f),
        length(s), arraySum(x -> toUInt8(x), s),
        length(i), arrayCount(x -> x IS NULL, i), arraySum(x -> assumeNotNull(x), i),
        length(ff[1]), arraySum(x -> toUInt8(x), ff[1])
    FROM file('$FILE', Parquet)
    ORDER BY n
"
