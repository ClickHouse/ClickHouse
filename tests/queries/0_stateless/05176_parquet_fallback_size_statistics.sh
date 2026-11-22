#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_fallback.parquet"
trap 'rm -f "$FILE"' EXIT

# A column chunk that grows past output_format_parquet_max_dictionary_size is encoded twice: the
# dictionary pass is discarded and the chunk starts over without one. Its size statistics must
# describe the values once, not once per pass.
$CLICKHOUSE_LOCAL --query "
    SELECT toString(number) AS s FROM numbers(1000)
    SETTINGS output_format_parquet_max_dictionary_size = 1
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --query "
    SELECT
        columns.have_size_statistics AS present,
        columns.size_statistics.unencoded_byte_array_data_bytes AS reported,
        reported = (SELECT sum(length(toString(number))) FROM numbers(1000)) AS matches_values
    FROM file('$FILE', ParquetMetadata)
    ARRAY JOIN row_groups
    ARRAY JOIN row_groups.columns AS columns
"

# The level histograms are filled a page at a time, so a discarded pass leaves its pages counted
# twice. That only shows on a column that has levels at all, and only when the dictionary outgrows
# its limit after some pages have been written - hence the small data page size.
$CLICKHOUSE_LOCAL --query "
    SELECT arrayMap(i -> if(i % 3 = 0, NULL, concat(toString(number * 10 + i), repeat('q', 200))), range(number % 4)) AS a
    FROM numbers(20000)
    SETTINGS output_format_parquet_data_page_size = 1024, output_format_parquet_max_dictionary_size = 1000000
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --query "
    SELECT
        columns.size_statistics.repetition_level_histogram AS rep,
        columns.size_statistics.definition_level_histogram AS def,
        columns.size_statistics.unencoded_byte_array_data_bytes AS bytes
    FROM file('$FILE', ParquetMetadata)
    ARRAY JOIN row_groups
    ARRAY JOIN row_groups.columns AS columns
"
