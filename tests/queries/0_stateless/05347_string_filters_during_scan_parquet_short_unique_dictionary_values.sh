#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The rows of dictionary-encoded Parquet pages are not checked one by one: every distinct dictionary
# entry is checked once to build a mask. But when every entry is referenced by a single row, building
# the mask costs as much as checking every row, so the string filter from PREWHERE must disable itself
# for short values, same as for the values that are checked one by one.

FILE="${CLICKHOUSE_DATABASE}/t_string_filter_short_unique_dict.parquet"

# Ten row groups of 10000 rows each, with a dictionary of 10000 distinct short values in each of them.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE', Parquet, 'id UInt32, s String')
SELECT number, toString(number)
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000;
"

for enable in 0 1; do
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(s)) FROM file('$FILE', Parquet) PREWHERE s LIKE '%x%' OR s = '12345' SETTINGS apply_string_filters_during_scan = $enable"
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(s)) FROM file('$FILE', Parquet) PREWHERE s LIKE '%123%' SETTINGS apply_string_filters_during_scan = $enable"
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE', Parquet) PREWHERE s LIKE '%123%' SETTINGS apply_string_filters_during_scan = 1, max_threads = 1, log_comment = '05347_short_unique_dictionary_values'"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# Almost all rows do not match. If the filter did not disable itself after the first dictionary entries,
# more than 90000 rows would be replaced.
$CLICKHOUSE_CLIENT -q "
SELECT sum(ProfileEvents['StringValueFilterValuesReplaced']) < 10000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05347_short_unique_dictionary_values';
"
