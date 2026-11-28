#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Compare row-store reconstruction with columnar join payloads. Numeric, decimal and fixed-string
# fields include null maps; unmatched left rows retain their type defaults, including nonzero enums.
# Switching an automatic join to merge join also reconstructs contiguous ranges of stored rows.
fields="number AS k, toInt8(number) AS i8, toUInt16(number) AS u16, toFloat32(number) AS f32,
    toUInt64(number) AS u64, toUInt128(number) AS u128, toInt256(number) AS i256,
    toDecimal32(number, 1) AS d32, toDecimal64(number, 2) AS d64,
    toDecimal128(number, 3) AS d128, toDecimal256(number, 4) AS d256,
    CAST('first', 'Enum8(\'first\' = 3, \'second\' = 7)') AS e"
for width in 1 2 3 4 5 6 7 8 16 32
do
    value="toFixedString(char(65 + number % 26), ${width})"
    fields+=", ${value} AS v_${width}, if(number % 3 = 0, NULL, ${value}) AS n_${width}"
done
fields+=", if(number % 3 = 0, NULL, toUInt64(number)) AS nu,
    if(number % 3 = 0, NULL, toDecimal128(number, 3)) AS nd"

for algorithm in hash auto
do
    join_limit=0
    if [[ "$algorithm" == auto ]]
    then
        join_limit=100000
    fi
    for kind in INNER LEFT
    do
        for row_store in 0 1
        do
            $CLICKHOUSE_LOCAL --query "
                SET join_algorithm = '${algorithm}', max_threads = 4, max_block_size = 1023;
                SET max_bytes_in_join = ${join_limit}, join_overflow_mode = 'break';
                SET query_plan_optimize_join_order_limit = 0, collect_hash_table_stats_during_joins = 0;
                SET min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_row_store = ${row_store};
                SELECT sum(cityHash64(*)) FROM
                    (SELECT r.* FROM (SELECT number % 3000 AS k FROM numbers(10000)) AS l
                     ${kind} JOIN (SELECT ${fields} FROM numbers(2000)) AS r ON l.k = r.k);
            " > "${CLICKHOUSE_TMP}/bulk_row_store_${row_store}.out"
        done
        diff -u "${CLICKHOUSE_TMP}/bulk_row_store_0.out" "${CLICKHOUSE_TMP}/bulk_row_store_1.out"
        echo "${algorithm} ${kind}"
    done
done
