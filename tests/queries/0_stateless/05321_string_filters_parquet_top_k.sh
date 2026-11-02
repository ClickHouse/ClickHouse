#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not supported in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The scan-time string filter (`apply_string_filters_during_scan`) replaces non-matching values with
# empty strings, and the row is rejected only after PREWHERE has been evaluated. The Parquet reader runs
# the TopN dynamic filter (`use_top_k_dynamic_filtering`) on the sort column as a step before PREWHERE,
# so it observes the substituted empty strings. This is fine: the filter only reads the running threshold
# (the sorting sets it from the rows that passed PREWHERE), and it can only drop rows, while the rows
# with substituted values are rejected by PREWHERE anyway. The result must not depend on the setting.

FILE="${CLICKHOUSE_DATABASE}/t_string_filter_top_k.parquet"

# The non-matching values are at the beginning of the file, in small row groups.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE', Parquet, 'id UInt32, s String')
SELECT number, if(number < 50000, 'nothing interesting ' || toString(number), 'lorem needle ipsum ' || toString(number))
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000, output_format_parquet_max_dictionary_size = 0;
"

for enable in 0 1; do
    $CLICKHOUSE_CLIENT -q "
    SELECT s FROM file('$FILE', Parquet) PREWHERE s LIKE '%needle%' ORDER BY s LIMIT 3
    SETTINGS apply_string_filters_during_scan = $enable, use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_block_size = 1000, max_threads = 1;
    SELECT s FROM file('$FILE', Parquet) PREWHERE s LIKE '%needle%' ORDER BY s DESC LIMIT 3
    SETTINGS apply_string_filters_during_scan = $enable, use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_block_size = 1000, max_threads = 1;
    "
done
