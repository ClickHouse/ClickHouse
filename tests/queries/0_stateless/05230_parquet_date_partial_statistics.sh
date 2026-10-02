#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Parquet format is not built in the fast-test image.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every field of row group statistics is optional. Each file holds one row group of parquet `DATE` values,
# one of them outside the `Date` range, under statistics that carry only one endpoint: only `min_value` for
# {2149-06-06, 2149-06-07}, only `max_value` for {1969-12-31, 1970-01-06}. Under the default
# `date_time_overflow_behavior` the read rejects that value, so the surviving endpoint must not prune the row
# group and turn the error into an empty result.
SETTINGS="input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1,
    input_format_parquet_page_filter_push_down = 0"
MIN_ONLY="$CUR_DIR/data_parquet/05230_date_min_only_statistics.parquet"
MAX_ONLY="$CUR_DIR/data_parquet/05230_date_max_only_statistics.parquet"

${CLICKHOUSE_LOCAL} --query "
SELECT count() FROM file('$MIN_ONLY', Parquet, 'x Date') WHERE x < toDate('2000-01-01')
SETTINGS $SETTINGS" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

${CLICKHOUSE_LOCAL} --query "
SELECT count() FROM file('$MAX_ONLY', Parquet, 'x Date') WHERE x > toDate('1970-01-10')
SETTINGS $SETTINGS" 2>&1 | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

# Under `saturate` the values arrive inside the range, so the surviving endpoint is a bound and must prune.
# This also shows that each file really carries that endpoint.
${CLICKHOUSE_LOCAL} --profile-events-delay-ms=-1 --print-profile-events --query "
SELECT count() FROM file('$MIN_ONLY', Parquet, 'x Date') WHERE x < toDate('2000-01-01')
SETTINGS $SETTINGS, date_time_overflow_behavior = 'saturate' FORMAT Null" |& grep -o -m1 'ParquetPrunedRowGroups: [0-9]*'

${CLICKHOUSE_LOCAL} --profile-events-delay-ms=-1 --print-profile-events --query "
SELECT count() FROM file('$MAX_ONLY', Parquet, 'x Date') WHERE x > toDate('1970-01-10')
SETTINGS $SETTINGS, date_time_overflow_behavior = 'saturate' FORMAT Null" |& grep -o -m1 'ParquetPrunedRowGroups: [0-9]*'
