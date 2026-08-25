#!/usr/bin/env bash
# Tags: no-fasttest
# Regression: when the Parquet footer is served from the shared metadata
# cache, the bloom-filter and dictionary pruning paths must not read the
# by-value file_metadata member that the cached flow never populates.
# Before the fix, the second (cache-hit) read of each file threw
# std::out_of_range in makeColumnDescriptor.
#
# The first read of each file populates the metadata cache, the second
# read gets the footer from the cache and exercises the pruning path.

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
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "$BF_QUERY SETTINGS $BF_SETTINGS" --query_id="${QID_PREFIX}bf2" > "$WORKING_DIR/bf2.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

diff "$WORKING_DIR/bf1.tsv" "$WORKING_DIR/bf2.tsv"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ParquetMetadataCacheHits'] > 0 FROM system.query_log WHERE query_id='${QID_PREFIX}bf2' AND type='QueryFinish' AND current_database = currentDatabase()"

# --- dictionary path: file with a dictionary-encoded column ---
DICT_FILE="${WORKING_DIR}/dict.parquet"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$DICT_FILE', Parquet, 's String, v UInt64') SELECT repeat(toString(number % 50), 8), number FROM numbers(1000) SETTINGS max_insert_threads=1, max_threads=1, engine_file_truncate_on_insert=1"

DICT_SETTINGS="use_parquet_metadata_cache=1, input_format_parquet_dictionary_filter_push_down=1048576, input_format_parquet_bloom_filter_push_down=0, use_query_condition_cache=0, max_threads=1"
DICT_QUERY="SELECT sum(v) FROM file('$DICT_FILE', Parquet) WHERE s = repeat(toString(7), 8) SETTINGS $DICT_SETTINGS"

$CLICKHOUSE_CLIENT -q "$DICT_QUERY" --query_id="${QID_PREFIX}dict1" > "$WORKING_DIR/dict1.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "$DICT_QUERY" --query_id="${QID_PREFIX}dict2" > "$WORKING_DIR/dict2.tsv"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

diff "$WORKING_DIR/dict1.tsv" "$WORKING_DIR/dict2.tsv"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['ParquetMetadataCacheHits'] > 0 FROM system.query_log WHERE query_id='${QID_PREFIX}dict2' AND type='QueryFinish' AND current_database = currentDatabase()"
