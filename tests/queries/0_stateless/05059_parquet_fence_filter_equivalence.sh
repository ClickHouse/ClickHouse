#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

WORK_DIR="$USER_FILES_PATH/$CLICKHOUSE_TEST_UNIQUE_NAME"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT
SETTINGS="input_format_parquet_bloom_filter_push_down=0, input_format_parquet_dictionary_filter_push_down=0, input_format_parquet_page_filter_push_down=0, use_query_condition_cache=0, optimize_move_to_prewhere=0, max_threads=1, max_parsing_threads=1"

check_case() {
    local name="$1" expression="$2" predicate="$3"
    local path="$WORK_DIR/$name.parquet"
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$path', Parquet) SELECT $expression AS k, number AS v FROM numbers(30) SETTINGS output_format_parquet_row_group_size=10, max_block_size=30, max_threads=1, max_insert_threads=1, engine_file_truncate_on_insert=1"
    local query="SELECT v FROM file('$path', Parquet) WHERE $predicate ORDER BY v"
    $CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS, input_format_parquet_filter_push_down=0" > "$WORK_DIR/expected.tsv"
    test -s "$WORK_DIR/expected.tsv"
    # Check both index construction and reuse against ordinary filtering.
    for _ in 1 2; do
        $CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS, input_format_parquet_filter_push_down=1" > "$WORK_DIR/actual.tsv"
        diff "$WORK_DIR/expected.tsv" "$WORK_DIR/actual.tsv"
    done
    echo "$name OK"
}

check_case ascending "number" "k=15"
check_case descending "29-number" "k=15"
check_case overlapping "intDiv(number,10)*5+number%10" "k=7"
check_case nullable "if(number%10=0, NULL, toInt64(number))" "k=15"
check_case decimal "toDecimal64(number,2)" "k=toDecimal64(15,2)"
check_case timestamp "toDateTime64('2025-01-01 00:00:00',3,'UTC')+toIntervalMillisecond(number)" "k=toDateTime64('2025-01-01 00:00:00.015',3,'UTC')"
check_case nan "if(number=29, toFloat64('nan'), toFloat64(number))" "k=15"
