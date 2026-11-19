#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column chunk that carries both a bloom filter and a dictionary page must be checked against the
# bloom filter first. The bloom filter has no false negatives, so when it rules the row group out the
# exact dictionary filter can only agree, and reading its (much larger) dictionary page is pure loss.
# Before this was ordered that way, `input_format_parquet_dictionary_filter_push_down` read every
# dictionary page of the file even when the bloom filters alone had already pruned every row group.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
DATA_FILE_NO_BLOOM="${CLICKHOUSE_TEST_UNIQUE_NAME}_no_bloom.parquet"

# 2 row groups of 20000 rows, every row with a distinct `category` value, so each row group's
# dictionary page holds 20000 entries and is far larger than the bloom filter that covers it.
# `output_format_parquet_max_dictionary_size` is raised so the writer keeps the column
# dictionary-encoded instead of falling back to PLAIN, and `max_block_size` /
# `output_format_parquet_row_group_size` are pinned so the row group boundaries are deterministic
# regardless of the randomized settings in CI.
write_file() {
    ${CLICKHOUSE_CLIENT} --query="
        insert into function file('$1', Parquet)
        select number as n, concat('rg', toString(intDiv(number, 20000)), '_val_', toString(number % 20000)) as category
        from numbers(40000)
        settings output_format_parquet_row_group_size = 20000, output_format_parquet_max_dictionary_size = 100000000,
                 output_format_parquet_write_bloom_filter = $2, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
    "
}
write_file "${DATA_FILE}" 1
write_file "${DATA_FILE_NO_BLOOM}" 0

# Disable the min/max and page filters so pruning happens only via the dictionary or the bloom filter,
# and keep a generous dictionary-page eligibility limit so the dictionary filter is always applicable.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

# `no_such_value` is in no row group, so the bloom filters alone rule out the whole file.
QUERY_ABSENT="select count() from file('${DATA_FILE}', Parquet) where category = 'no_such_value'"

# Runs the query and reports the bytes it read from the file, via the query log.
bytes_read() {
    local query_id="${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
    ${CH} --query_id="${query_id}" --input_format_parquet_dictionary_filter_push_down="$1" --query="$2 FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query="
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '${query_id}' AND type = 'QueryFinish' AND current_database = currentDatabase();
    "
}

# Warm up the Parquet metadata cache so both measurements below read the footer the same way.
${CH} --query="${QUERY_ABSENT} FORMAT Null"

without_filter=$(bytes_read 0 "${QUERY_ABSENT}")
with_filter=$(bytes_read 100000000 "${QUERY_ABSENT}")

echo "the bloom filters rule out every row group, so the dictionary filter reads no extra bytes"
[ "${with_filter}" -le "${without_filter}" ] && echo "OK" || echo "FAIL: ${with_filter} > ${without_filter}"

# Prints the query result and the number of rows read from the file.
rows_read() {
    ${CH} --input_format_parquet_dictionary_filter_push_down="$1" --query="$2 FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "with bloom filters, pruning is unchanged: nothing is read either way"
rows_read 0 "${QUERY_ABSENT}"
rows_read 100000000 "${QUERY_ABSENT}"

echo "without bloom filters, the dictionary filter is still what prunes the file"
QUERY_NO_BLOOM="select count() from file('${DATA_FILE_NO_BLOOM}', Parquet) where category = 'no_such_value'"
rows_read 0 "${QUERY_NO_BLOOM}"
rows_read 100000000 "${QUERY_NO_BLOOM}"

echo "a value that is in one row group only is still found, and the other row group is still pruned"
QUERY_PRESENT="select count() from file('${DATA_FILE}', Parquet) where category = 'rg0_val_5'"
rows_read 0 "${QUERY_PRESENT}"
rows_read 100000000 "${QUERY_PRESENT}"
