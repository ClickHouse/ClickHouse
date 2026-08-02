#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_data.parquet"
CORRUPT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_corrupt.parquet"
LEGACY_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy.parquet"
PAGE_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pages.parquet"
CORRUPT_PAGE_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_corrupt_pages.parquet"
LEGACY_PAGE_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy_pages.parquet"
trap 'rm -f "${DATA_FILE}" "${CORRUPT_FILE}" "${LEGACY_FILE}" "${PAGE_FILE}" "${CORRUPT_PAGE_FILE}" "${LEGACY_PAGE_FILE}"' EXIT

# Four row groups whose numeric ranges are disjoint but whose little-endian byte ranges are not
# useful as numeric bounds. The last group also reaches the maximum value of each type.
${CLICKHOUSE_LOCAL} --query="
    SELECT
        multiIf(
            intDiv(number, 1000) = 0, toUInt128(number % 1000),
            intDiv(number, 1000) = 1, bitShiftLeft(toUInt128(1), 64) + toUInt128(number % 1000),
            intDiv(number, 1000) = 2, bitShiftLeft(toUInt128(1), 127) + toUInt128(number % 1000),
            toUInt128(toUInt128('340282366920938463463374607431768211455') - toUInt128(999) + toUInt128(number % 1000))) AS u128,
        multiIf(
            intDiv(number, 1000) = 0, toUInt256(number % 1000),
            intDiv(number, 1000) = 1, bitShiftLeft(toUInt256(1), 128) + toUInt256(number % 1000),
            intDiv(number, 1000) = 2, bitShiftLeft(toUInt256(1), 255) + toUInt256(number % 1000),
            toUInt256(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') - toUInt256(999) + toUInt256(number % 1000))) AS u256,
        if(number % 10 = 0, NULL, u128)::Nullable(UInt128) AS u128_nullable,
        if(intDiv(number, 1000) = 0, NULL, u256)::Nullable(UInt256) AS u256_nullable,
        number AS n
    FROM numbers(4000)
    SETTINGS
        output_format_parquet_row_group_size = 1000,
        output_format_parquet_max_dictionary_size = 0,
        max_block_size = 1000000
    FORMAT Parquet
" > "${DATA_FILE}"

# One row group with 16 pages per wide-integer column. The final nullable page is all null.
${CLICKHOUSE_LOCAL} --query="
    SELECT
        bitShiftLeft(toUInt128(1), 127) + toUInt128(number) AS u128,
        bitShiftLeft(toUInt256(1), 255) + toUInt256(number) AS u256,
        if(number >= 3840, NULL, u256)::Nullable(UInt256) AS u256_nullable,
        number AS n
    FROM numbers(4096)
    SETTINGS
        output_format_parquet_row_group_size = 100000,
        output_format_parquet_data_page_size = 4096,
        output_format_parquet_batch_size = 256,
        output_format_parquet_max_dictionary_size = 0,
        output_format_parquet_write_page_index = 1,
        max_block_size = 1000000
    FORMAT Parquet
" > "${PAGE_FILE}"

STRUCTURE="u128 UInt128, u256 UInt256, u128_nullable Nullable(UInt128), u256_nullable Nullable(UInt256), n UInt64"
RAW_STRUCTURE="u128 FixedString(16), u256 FixedString(32), u128_nullable Nullable(FixedString(16)), u256_nullable Nullable(FixedString(32)), n UInt64"
PAGE_STRUCTURE="u128 UInt128, u256 UInt256, u256_nullable Nullable(UInt256), n UInt64"

echo "round trip and numeric boundary values"
${CLICKHOUSE_LOCAL} --multiquery --query="
    SELECT count(), min(n), max(n)
    FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 BETWEEN bitShiftLeft(toUInt128(1), 127) + 100 AND bitShiftLeft(toUInt128(1), 127) + 199;
    SELECT count(), min(n), max(n)
    FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u256 BETWEEN bitShiftLeft(toUInt256(1), 255) + 100 AND bitShiftLeft(toUInt256(1), 255) + 199;
    SELECT min(u128), max(u128), min(u256), max(u256)
    FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}');
"

echo "legacy little-endian payload"
${CLICKHOUSE_LOCAL} --query="
    SELECT hex(u128), hex(u256)
    FROM file('${DATA_FILE}', Parquet, '${RAW_STRUCTURE}')
    WHERE n = 1001;
"

CH="${CLICKHOUSE_LOCAL} --input_format_parquet_filter_push_down=1 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

profile_row_groups() {
    local query="$1"
    ${CH} --print-profile-events --query="${query} FORMAT Null" 2>&1 | awk '
        /ParquetReadRowGroups:/   { read += $(NF-1) }
        /ParquetPrunedRowGroups:/ { pruned += $(NF-1) }
        END { print "read=" read+0 " pruned=" pruned+0 }
    '
}

profile_pruned_pages() {
    local query="$1"
    local profile
    if ! profile=$(${CLICKHOUSE_LOCAL} \
            --input_format_parquet_filter_push_down=1 \
            --input_format_parquet_page_filter_push_down=1 \
            --input_format_parquet_bloom_filter_push_down=0 \
            --input_format_parquet_use_offset_index=0 \
            --optimize_move_to_prewhere=0 \
            --use_cache_for_count_from_files=0 \
            --print-profile-events \
            --query="${query} FORMAT Null" 2>&1); then
        echo "${profile}" >&2
        return 1
    fi
    awk '
        /ParquetPrunedPages:/ { pruned += $(NF-1) }
        END { print "pruned=" pruned+0 }
    ' <<< "${profile}"
}

echo "UInt128 numeric statistics prune three row groups"
profile_row_groups "
    SELECT * FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 BETWEEN bitShiftLeft(toUInt128(1), 127) + 100 AND bitShiftLeft(toUInt128(1), 127) + 199"

echo "UInt256 numeric statistics prune three row groups"
profile_row_groups "
    SELECT * FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u256 BETWEEN bitShiftLeft(toUInt256(1), 255) + 100 AND bitShiftLeft(toUInt256(1), 255) + 199"

echo "nullable UInt128 statistics"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128_nullable = bitShiftLeft(toUInt128(1), 127) + 101;
"

echo "all-null UInt256 row group and numeric statistics prune every row group"
${CLICKHOUSE_LOCAL} --query="
    SELECT count()
    FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u256_nullable = toUInt256(1);
"
profile_row_groups "
    SELECT * FROM file('${DATA_FILE}', Parquet, '${STRUCTURE}')
    WHERE u256_nullable = toUInt256(1)"

echo "UInt128 numeric page statistics prune fifteen pages"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(2100)
"
profile_pruned_pages "
    SELECT * FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(2100)"

echo "UInt256 numeric page statistics prune fifteen pages"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256 = bitShiftLeft(toUInt256(1), 255) + toUInt256(2100)
"
profile_pruned_pages "
    SELECT * FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256 = bitShiftLeft(toUInt256(1), 255) + toUInt256(2100)"

echo "nullable UInt256 numeric page statistics include an all-null page"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256_nullable = bitShiftLeft(toUInt256(1), 255) + toUInt256(300)
"
profile_pruned_pages "
    SELECT * FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256_nullable = bitShiftLeft(toUInt256(1), 255) + toUInt256(300)"

echo "files without numeric page statistics retain legacy behavior"
cp "${PAGE_FILE}" "${LEGACY_PAGE_FILE}"
perl -0pi -e 's/clickhouse\.wide_integer_page_statistics/xlickhouse.wide_integer_page_statistics/g' "${LEGACY_PAGE_FILE}"
profile_pruned_pages "
    SELECT * FROM file('${LEGACY_PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(2100)"

echo "files without numeric statistics retain legacy behavior"
cp "${DATA_FILE}" "${LEGACY_FILE}"
perl -0pi -e 's/clickhouse\.wide_integer_statistics/xlickhouse.wide_integer_statistics/g' "${LEGACY_FILE}"
perl -0pi -e 's/clickhouse\.wide_integer_page_statistics/xlickhouse.wide_integer_page_statistics/g' "${LEGACY_FILE}"
${CLICKHOUSE_LOCAL} --query="
    SELECT count()
    FROM file('${LEGACY_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 BETWEEN bitShiftLeft(toUInt128(1), 127) + 100 AND bitShiftLeft(toUInt128(1), 127) + 199;
"
profile_row_groups "
    SELECT * FROM file('${LEGACY_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 BETWEEN bitShiftLeft(toUInt128(1), 127) + 100 AND bitShiftLeft(toUInt128(1), 127) + 199"

echo "malformed numeric statistics are rejected"
cp "${DATA_FILE}" "${CORRUPT_FILE}"
perl -0pi -e 's/1;UInt128;/9;UInt128;/' "${CORRUPT_FILE}"
if error=$(${CLICKHOUSE_LOCAL} --query="
    SELECT count()
    FROM file('${CORRUPT_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 = toUInt128(1)
" 2>&1); then
    echo "unexpected success"
elif grep -q 'Unsupported.*wide_integer_statistics.*version' <<< "${error}"; then
    echo "rejected"
else
    echo "${error}"
fi

echo "malformed numeric page statistics are rejected"
cp "${PAGE_FILE}" "${CORRUPT_PAGE_FILE}"
perl -0pi -e 's/1;UInt128;/9;UInt128;/g' "${CORRUPT_PAGE_FILE}"
if error=$(${CLICKHOUSE_LOCAL} \
    --input_format_parquet_filter_push_down=0 \
    --input_format_parquet_page_filter_push_down=1 \
    --input_format_parquet_bloom_filter_push_down=0 \
    --query="
        SELECT *
        FROM file('${CORRUPT_PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
        WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(2100)
        FORMAT Null
    " 2>&1); then
    echo "unexpected success"
elif grep -q 'Unsupported.*wide_integer_page_statistics.*version' <<< "${error}"; then
    echo "rejected"
else
    echo "${error}"
fi
