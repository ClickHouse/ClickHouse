#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires the ORC format (USE_ORC build option)
#
# Regression test: schema inference wrapped map KEYS in Nullable inside LowCardinality
# (a dictionary-encoded ORC map key is inferred as `LowCardinality(String)`, and
# `makeNullableRecursively` turned it into `LowCardinality(Nullable(String))`), so reading
# the file failed with `Map cannot have a key of type LowCardinality(Nullable(String))` —
# including files ClickHouse itself wrote, and Spark-written ORC with map columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/04545_dict_map_${CLICKHOUSE_DATABASE}.orc"
trap 'rm -f "${FILE}"' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT map(concat('key_', toString(number % 3)), number) AS m
    FROM numbers(100)
    INTO OUTFILE '${FILE}'
    FORMAT ORC
    SETTINGS output_format_orc_dictionary_key_size_threshold = 1
"

echo '--- inferred schema ---'
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${FILE}')"

echo '--- read back ---'
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(m['key_1']) FROM file('${FILE}')"
