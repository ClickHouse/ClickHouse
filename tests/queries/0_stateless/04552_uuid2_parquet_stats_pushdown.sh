#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet support is not compiled into the fast-test build.

# Row-group and page min/max statistics of a Parquet column read as `UUID2` must be decoded
# in the `UUID2` (canonical big-endian) layout. Decoding them in the historical `UUID` layout
# would order differently, so filter pushdown could silently prune row groups and pages
# that contain matching rows.
# The values below are chosen so that the two layouts order in opposite directions
# (the first half of the canonical text grows while the last one shrinks and vice versa).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
FILE_ROW_GROUPS="${CLICKHOUSE_TEST_UNIQUE_NAME}/row_groups.parquet"
FILE_PAGES="${CLICKHOUSE_TEST_UNIQUE_NAME}/pages.parquet"
rm -f "${CLICKHOUSE_USER_FILES_UNIQUE}"/*.parquet

# Two row groups of two rows each.
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('${FILE_ROW_GROUPS}', Parquet, 'u UUID2')
    SELECT arrayJoin([
        '20000000-0000-4000-8000-100000000000',
        '30000000-0000-4000-8000-200000000000',
        '80000000-0000-4000-8000-300000000000',
        '90000000-0000-4000-8000-400000000000'])::UUID2
    SETTINGS output_format_parquet_row_group_size = 2, engine_file_truncate_on_insert = 1;
"

echo 'row group pruning'
query_id="${CLICKHOUSE_DATABASE}_04552_uuid2_row_group_prune_$RANDOM"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query="
    SELECT count() FROM file('${FILE_ROW_GROUPS}', Parquet, 'u UUID2')
    WHERE u = '30000000-0000-4000-8000-200000000000'::UUID2
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM file('${FILE_ROW_GROUPS}', Parquet, 'u UUID2')
    WHERE u >= '80000000-0000-4000-8000-000000000000'::UUID2
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
"
# The same file must also read back correctly as `UUID` (same canonical text, different layout).
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM file('${FILE_ROW_GROUPS}', Parquet, 'u UUID')
    WHERE u = toUUID('30000000-0000-4000-8000-200000000000')
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
"

# Prove that the matching row group is kept and the non-matching one is actually pruned.
${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS query_log;"
${CLICKHOUSE_CLIENT} --query="
    SELECT ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    FROM system.query_log
    WHERE event_date >= yesterday() AND query_id = '${query_id}' AND type = 'QueryFinish' AND current_database = currentDatabase();
"

echo 'plain fixed-length byte array'
# FLBA(16) without the logical UUID annotation, read with an explicit `UUID2` hint.
FILE_FLBA="${CLICKHOUSE_TEST_UNIQUE_NAME}/flba.parquet"
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('${FILE_FLBA}', Parquet, 'u FixedString(16)')
    SELECT UUIDStringToNum(arrayJoin([
        '20000000-0000-4000-8000-100000000000',
        '30000000-0000-4000-8000-200000000000',
        '80000000-0000-4000-8000-300000000000',
        '90000000-0000-4000-8000-400000000000']))
    SETTINGS output_format_parquet_row_group_size = 2, engine_file_truncate_on_insert = 1;
"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM file('${FILE_FLBA}', Parquet, 'u UUID2')
    WHERE u = '30000000-0000-4000-8000-200000000000'::UUID2
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
"

echo 'page pruning'
# One row group with many small pages; the page index must be decoded in the UUID2 layout too.
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('${FILE_PAGES}', Parquet, 'u UUID2')
    SELECT concat(lower(hex(268435456 + number)), '-0000-4000-8000-', lower(lpad(hex(1099511627776 - number), 12, '0')))::UUID2
    FROM numbers(100)
    SETTINGS output_format_parquet_data_page_size = 100, output_format_parquet_write_page_index = 1, engine_file_truncate_on_insert = 1;
"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM file('${FILE_PAGES}', Parquet, 'u UUID2')
    WHERE u = '10000032-0000-4000-8000-00ffffffffce'::UUID2
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM file('${FILE_PAGES}', Parquet, 'u UUID2')
    WHERE u > '10000050-0000-4000-8000-000000000000'::UUID2
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
