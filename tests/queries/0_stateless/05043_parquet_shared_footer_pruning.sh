#!/usr/bin/env bash
# Tags: no-fasttest
# Regression: a cache-hit footer left the by-value file_metadata member
# empty, and the bloom/dictionary pruning paths read it (std::out_of_range
# in makeColumnDescriptor). The second read of each file gets the footer
# from the cache and must produce the same rows as the first.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QID_PREFIX="parquet_shared_footer_${$}_"
WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$WORKING_DIR"
trap 'rm -rf "$WORKING_DIR"' EXIT

# --- bloom filter path: pre-built file with bloom filters ---
BF_FILE="${WORKING_DIR}/multi_column_bf.gz.parquet"
cp "${CUR_DIR}/data_parquet/multi_column_bf.gz.parquet" "$BF_FILE"

BF_SETTINGS="use_parquet_metadata_cache=1, input_format_parquet_bloom_filter_push_down=1, input_format_parquet_dictionary_filter_push_down=0, use_query_condition_cache=0, max_threads=1"
BF_QUERY="SELECT string, flba FROM file('$BF_FILE', Parquet) WHERE string='PFJH' ORDER BY uint16_logical FORMAT TSV"

$CLICKHOUSE_CLIENT -q "$BF_QUERY SETTINGS $BF_SETTINGS" --query_id="${QID_PREFIX}bf1" > "$WORKING_DIR/bf1.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "$BF_QUERY SETTINGS $BF_SETTINGS" --query_id="${QID_PREFIX}bf2" > "$WORKING_DIR/bf2.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

diff "$WORKING_DIR/bf1.tsv" "$WORKING_DIR/bf2.tsv"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ParquetMetadataCacheHits'] > 0 FROM system.query_log WHERE query_id='${QID_PREFIX}bf2' AND type='QueryFinish' AND current_database = currentDatabase()"

# Three deterministic row groups with disjoint, small dictionaries. Disable
# the other filters so a reduced `read_rows` proves dictionary pruning ran.
DICT_FILE="${WORKING_DIR}/dict.parquet"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$DICT_FILE', Parquet, 's String, v UInt64') SELECT toString(intDiv(number, 10000) * 1000 + number % 50), number FROM numbers(30000) SETTINGS output_format_parquet_row_group_size=10000, max_block_size=1000000, max_insert_threads=1, max_threads=1, engine_file_truncate_on_insert=1"

DICT_SETTINGS="input_format_parquet_filter_push_down=0, input_format_parquet_page_filter_push_down=0, input_format_parquet_bloom_filter_push_down=0, optimize_move_to_prewhere=0, use_cache_for_count_from_files=0, use_query_condition_cache=0, max_threads=1, max_parsing_threads=1"
DICT_QUERY="SELECT count(), sum(v) FROM file('$DICT_FILE', Parquet) WHERE s = '1007'"

$CLICKHOUSE_CLIENT -q "$DICT_QUERY SETTINGS $DICT_SETTINGS, use_parquet_metadata_cache=0, input_format_parquet_dictionary_filter_push_down=0" > "$WORKING_DIR/dict_baseline.tsv"
diff <(printf '200\t2996400\n') "$WORKING_DIR/dict_baseline.tsv"
$CLICKHOUSE_CLIENT -q "$DICT_QUERY SETTINGS $DICT_SETTINGS, use_parquet_metadata_cache=1, input_format_parquet_dictionary_filter_push_down=1048576" --query_id="${QID_PREFIX}dict1" > "$WORKING_DIR/dict1.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "$DICT_QUERY SETTINGS $DICT_SETTINGS, use_parquet_metadata_cache=1, input_format_parquet_dictionary_filter_push_down=1048576" --query_id="${QID_PREFIX}dict2" > "$WORKING_DIR/dict2.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

diff "$WORKING_DIR/dict_baseline.tsv" "$WORKING_DIR/dict1.tsv"
diff "$WORKING_DIR/dict1.tsv" "$WORKING_DIR/dict2.tsv"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ParquetMetadataCacheHits'] > 0 AND read_rows = 10000 FROM system.query_log WHERE query_id='${QID_PREFIX}dict2' AND type='QueryFinish' AND current_database = currentDatabase()"
