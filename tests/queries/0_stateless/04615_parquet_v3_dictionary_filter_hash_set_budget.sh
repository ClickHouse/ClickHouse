#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# The value-set reservation in `hashDictionaryValues` must track what the pruning path really
# allocates: the vector of dictionary-value hashes (sorted in place; exactly 8 bytes per entry),
# plus the extra default-value hash kept outside the vector under `input_format_null_as_default`.
# This test pins `input_format_parquet_memory_high_watermark` on both sides of the boundary where
# the value set stops fitting the pruning budget, for two row groups of 16384 distinct values each.
# If the reservation regresses to an underestimate (say, stops charging the vector), the tighter
# watermark would keep pruning; if it regresses to an overestimate (say, back to the hash-table
# model this replaced, ~4 cells per value plus the vector), the looser watermark would stop
# pruning. Either flip changes `rows_read` and fails this test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
NULLS_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_nulls.parquet"

# 2 row groups of 16384 rows. Every row has a distinct Int32 `category`, so each row group's
# dictionary holds 16384 entries and its value set reserves a 128 KiB hash vector.
# `output_format_parquet_max_dictionary_size` is raised so the writer keeps the column
# dictionary-encoded; `max_block_size` / `output_format_parquet_row_group_size` pin the row group
# boundaries deterministically.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${DATA_FILE}', Parquet)
    select number as n, toInt32(intDiv(number, 16384) * 100000 + (number % 16384)) as category
    from numbers(32768)
    settings output_format_parquet_row_group_size = 16384, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

# Isolate the dictionary filter and keep it always applicable. The watermark is budgeted per reader
# stream, so pin the parsing parallelism to keep the per-reader share deterministic.
CH="${CLICKHOUSE_CLIENT} --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0 --input_format_parquet_dictionary_filter_push_down=100000000 --max_threads=1 --max_parsing_threads=1"

run() {
    local watermark="$1"
    local query="$2"
    shift 2
    ${CH} --input_format_parquet_memory_high_watermark="${watermark}" "$@" --query="${query} FORMAT JSON" \
        | jq -c '{result: .data, rows_read: .statistics.rows_read}'
}

echo "generous memory budget: the dictionary filter prunes row group 1, only 16384 rows are read"
run 4000000000 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "watermark that affords the hash vectors: the dictionary filter still prunes, only 16384 rows are read"
run 6000000 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "watermark just below what the value set needs: pruning must be skipped, all 32768 rows are read"
run 4000000 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "extreme memory budget (1 byte): pruning is skipped, result is still correct"
run 1 "select count() from file('${DATA_FILE}', Parquet) where category = 5"

echo "results are identical regardless of the memory budget"
diff \
    <(${CH} --input_format_parquet_memory_high_watermark=4000000000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in (5, 100007, 100, 116383) group by category order by category") \
    <(${CH} --input_format_parquet_memory_high_watermark=6000000 --query="select category, count(), sum(n) from file('${DATA_FILE}', Parquet) where category in (5, 100007, 100, 116383) group by category order by category") \
    && echo "OK"

# The extra default-value hash added under `input_format_null_as_default`: row group 0 holds 4096
# distinct non-null values plus 256 nulls; row group 1 holds 4352 distinct non-null values and no
# nulls. Values start at 1, so the type default 0 matches only
# the nulls of row group 0: querying it must read row group 0 (its nulls decode to 0) while still
# pruning the null-free row group 1, and querying a value that exists only in row group 1 must prune
# row group 0 even though its value set was extended with the default.
${CLICKHOUSE_CLIENT} --query="
    insert into function file('${NULLS_FILE}', Parquet)
    select
        number as n,
        if(number < 4352 and (number % 4352) % 17 = 16,
           NULL,
           toInt32(intDiv(number, 4352) * 100000 + ((number % 4352) - intDiv(number % 4352, 17)) + 1)) as category
    from numbers(8704)
    settings output_format_parquet_row_group_size = 4352, output_format_parquet_max_dictionary_size = 100000000, engine_file_truncate_on_insert = 1, max_block_size = 1000000;
"

echo "null_as_default: querying the default value reads the row group with nulls (256 matches) and prunes the null-free one"
run 4000000000 "select count() from file('${NULLS_FILE}', Parquet, 'n UInt64, category Int32') where category = 0" --input_format_null_as_default=1

echo "null_as_default: a value only in row group 1 prunes row group 0 despite its default-extended value set"
run 4000000000 "select count() from file('${NULLS_FILE}', Parquet, 'n UInt64, category Int32') where category = 100005" --input_format_null_as_default=1

# The nullable file gets the same both-sides watermark check as the non-null one above, so the
# accounting of the nullable path (where the value set is extended with the default value hash) is
# pinned too, not just its results: a watermark that affords the hash vectors must keep pruning, and
# one below what they need must fall back to a full scan. Note that the "the default value hash stays
# out of the vector" invariant itself cannot be pinned by a watermark: for this `FixedSize` dictionary
# the reservation also covers the materialization terms, which leave enough headroom for a doubled
# vector, so appending would not change the behavior here. It is pinned by a `chassert` in
# `hashDictionaryValues` instead, which every debug and sanitizer build checks on this very query.
echo "null_as_default, watermark that affords the hash vectors: the null-free row group is still pruned"
run 1100000 "select count() from file('${NULLS_FILE}', Parquet, 'n UInt64, category Int32') where category = 0" --input_format_null_as_default=1

echo "null_as_default, watermark just below what the value set needs: pruning is skipped, all 8704 rows are read"
run 975000 "select count() from file('${NULLS_FILE}', Parquet, 'n UInt64, category Int32') where category = 0" --input_format_null_as_default=1

echo "null_as_default results are identical regardless of the memory budget"
diff \
    <(${CH} --input_format_null_as_default=1 --input_format_parquet_memory_high_watermark=4000000000 --query="select count(), sum(n) from file('${NULLS_FILE}', Parquet, 'n UInt64, category Int32') where category in (0, 3, 100005)") \
    <(${CH} --input_format_null_as_default=1 --input_format_parquet_memory_high_watermark=1 --query="select count(), sum(n) from file('${NULLS_FILE}', Parquet, 'n UInt64, category Int32') where category in (0, 3, 100005)") \
    && echo "OK"
