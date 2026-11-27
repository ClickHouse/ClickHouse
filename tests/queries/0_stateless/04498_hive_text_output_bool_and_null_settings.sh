#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# HiveText output must stay compatible with Hive's LazySimpleSerDe regardless of unrelated generic
# ClickHouse text settings. A visible fields/collection delimiter keeps the reference readable.

# Bool must always be `true`/`false`, never the bool_true_representation / bool_false_representation tokens.
${CLICKHOUSE_CLIENT} --query "SELECT CAST(true, 'Bool') AS t, CAST(false, 'Bool') AS f
    FORMAT HiveText
    SETTINGS bool_true_representation = 'yes', bool_false_representation = 'no',
             input_format_hive_text_fields_delimiter = ','"

# NULL must always be Hive's default null sequence \N, never format_csv_null_representation.
${CLICKHOUSE_CLIENT} --query "SELECT CAST(NULL, 'Nullable(Int32)') AS n, CAST(42, 'Nullable(Int32)') AS v
    FORMAT HiveText
    SETTINGS format_csv_null_representation = 'CUSTOM_NULL',
             input_format_hive_text_fields_delimiter = ','"

# The same must hold for nulls nested inside collections.
${CLICKHOUSE_CLIENT} --query "SELECT CAST([1, NULL, 3], 'Array(Nullable(Int32))') AS a
    FORMAT HiveText
    SETTINGS format_csv_null_representation = 'CUSTOM_NULL',
             input_format_hive_text_collection_items_delimiter = ';'"
