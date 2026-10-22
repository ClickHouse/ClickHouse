#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not supported in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The string filter from PREWHERE disables itself when the skipped values are too short to pay off
# for the check of every value. Rows of dictionary-encoded Parquet pages are not checked one by one:
# every distinct dictionary entry is checked once, and the rows only look up the result in a mask.
# So the filter must stay enabled for short low-cardinality values in such pages.

FILE="${CLICKHOUSE_DATABASE}/t_string_filter_short_dict.parquet"

# Ten row groups of 10000 rows each, with a dictionary of three one-byte values.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE', Parquet, 'id UInt32, s String')
SELECT number, ['A', 'N', 'R'][number % 3 + 1]
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000;
"

# A dictionary-encoded column read only by the last PREWHERE step is evaluated once per dictionary value,
# without the filter: `id < 100000` (true for all rows) moves `s` to an earlier step.
for enable in 0 1; do
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(s)) FROM file('$FILE', Parquet) PREWHERE s LIKE '%R%' AND id < 100000 SETTINGS apply_string_filters_during_scan = $enable"
done

# A column that can be written to the columns cache is decoded without the filter, so that the cache gets its values.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE', Parquet) PREWHERE s LIKE '%R%' AND id < 100000 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05338_short_dictionary_values'"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# Two thirds of the rows do not match. If the filter disabled itself after the first row group,
# less than 10000 rows would be replaced.
$CLICKHOUSE_CLIENT -q "
SELECT sum(ProfileEvents['StringValueFilterValuesReplaced']) > 50000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05338_short_dictionary_values';
"
