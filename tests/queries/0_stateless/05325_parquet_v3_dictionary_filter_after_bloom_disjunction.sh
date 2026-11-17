#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In `a = 'absent' OR b IN (...)` the bloom filter of `a` proves `a = 'absent'` false, but the row
# group survives the bloom pass because `b`'s branch stays unresolved. Then only `b`'s dictionary page
# can still change the outcome: `a`'s dictionary would only confirm the miss, so it must not be read.

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# One row group of 20000 rows with distinct, poorly compressible values in both columns, so each
# dictionary page is large even after compression. `output_format_parquet_max_dictionary_size` is
# raised so the writer keeps both columns dictionary-encoded instead of falling back to PLAIN.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select concat('a_', hex(sipHash128(number, 1))) as a, concat('b_', hex(sipHash128(number, 2))) as b
    from numbers(20000)
    settings output_format_parquet_row_group_size = 20000, output_format_parquet_max_dictionary_size = 100000000,
             output_format_parquet_write_bloom_filter = 1, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Disable the min/max and page filters so pruning happens only via the dictionary or the bloom filter.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000"

# An `IN` set larger than the bloom filter set-size cap (100) is checked against the dictionary only,
# so the bloom filter of `b` cannot rule the row group out, while its dictionary can.
B_SET=$(seq 1 200 | sed "s/.*/'no_such_b_&'/" | paste -sd,)

QUERY_B="select count() from file('${DATA_FILE}', Parquet) where b in (${B_SET})"
QUERY_A_OR_B="select count() from file('${DATA_FILE}', Parquet) where a = 'no_such_a' or b in (${B_SET})"

# Runs the query and reports the bytes it read from the file, via the query log.
bytes_read() {
    local query_id="${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
    ${CH} --query_id="${query_id}" --query="$1 FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query="
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '${query_id}' AND type = 'QueryFinish' AND current_database = currentDatabase();
    "
}

# Prints the query result and the number of rows read from the file.
rows_read() {
    ${CH} --query="$1 FORMAT JSON" | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

# Warm up the Parquet metadata cache so the measurements below read the footer the same way.
${CH} --query="${QUERY_B} FORMAT Null"

echo "both queries are pruned by the dictionary of b"
rows_read "${QUERY_B}"
rows_read "${QUERY_A_OR_B}"

bytes_b=$(bytes_read "${QUERY_B}")
bytes_a_or_b=$(bytes_read "${QUERY_A_OR_B}")

# The dictionary page of `a` holds 20000 random 34-character strings, several hundred KB even after
# compression. Reading it would add at least that much; the bloom filter blocks of `a` that the second
# query also reads are a few hundred bytes.
echo "the dictionary page of a is not read"
[ $((bytes_a_or_b - bytes_b)) -lt 100000 ] && echo "OK" || echo "FAIL: ${bytes_a_or_b} vs ${bytes_b}"

echo "a value present in a is still found"
${CH} --query="select count() from file('${DATA_FILE}', Parquet) where a = concat('a_', hex(sipHash128(toUInt64(5), 1))) or b in (${B_SET})"
