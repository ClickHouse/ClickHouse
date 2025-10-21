#!/usr/bin/env bash
# Tags: long, no-fasttest, no-asan, no-msan, no-tsan
#asdqwe no-debug

# Load various .parquet files from the internet, and files used by other tests.

# userdata{1..5}.parquet are from:
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata1.parquet
# ...
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata5.parquet

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_parquet

# TODO [parquet]: Known issues to investigate:

# ClickHouse Parquet reader doesn't support such complex types, so I didn't burrow into the issue.
# There is failure due parsing nested arrays or nested maps with NULLs:
# ../contrib/arrow/cpp/src/arrow/array/array_nested.cc:192:  Check failed: (self->list_type_->value_type()->id()) == (data->child_data[0]->type->id())

# Strange behaviour for repeated_no_annotation.parquet around __buitin_expect, so this file was disabled:
# debug:
#   ../contrib/arrow/cpp/src/arrow/array/array_nested.cc:193:  Check failed: self->list_type_->value_type()->Equals(data->child_data[0]->type)
# release:
#   Code: 349. DB::Ex---tion: Can not insert NULL data into non-nullable column "phoneNumbers": data for INSERT was parsed from stdin

EXCLUDE=(
    # These have extremely long strings and blow up the output.
    list_monotonically_increasing_offsets.parquet
    string_int_list_inconsistent_offset_multiple_batches.parquet
    # Date out of range.
    02716_data.parquet
)

for NAME in $(find "$DATA_DIR"/*.parquet -print0 | xargs -0 -n 1 basename | LC_ALL=C sort | grep -vFf <(printf '%s\n' "${EXCLUDE[@]}")); do
    echo "=== Try load data from $NAME"

    # We want to read the file once and get both a hash of the whole data and a sample of a few
    # pseudorandomly chosen rows. We use a dummy GROUP BY WITH TOTALS for that.
    # (Maybe the unnecessary GROUP BY could be expensive if there are lots of rows, but these test
    #  files don't have lots of rows or they would be too big to check into git.)
    # TODO [parquet]: Delete the input_format_parquet_enable_json_parsing=0 when cityHash64
    #                 supports JSON: https://github.com/ClickHouse/ClickHouse/issues/87734
    # TODO [parquet]: Delete the session_timezone='UTC' after https://github.com/ClickHouse/ClickHouse/pull/87872
    ${CLICKHOUSE_LOCAL} --query="
        SELECT _row_number, *, count(), sum(cityHash64(_row_number, *)) FROM file('$DATA_DIR/$NAME') GROUP BY all WITH TOTALS ORDER BY cityHash64(_row_number) LIMIT 20 SETTINGS input_format_parquet_enable_json_parsing=0, session_timezone='UTC';" 2>&1
done
