#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Validate that date_time_input_format='best_effort' is the default for row input formats.
# Counterpart to 04070 (which covers function/CAST paths via cast_string_to_date_time_mode).

echo '-- CSV: best_effort by default parses a non-basic datetime'
echo '"Apr 15, 2020 10:30:00"' | ${CLICKHOUSE_LOCAL} --query "SELECT t FROM table" --structure "t DateTime('UTC')" --input-format CSV

echo '-- CSV: basic mode rejects the same non-basic datetime'
echo '"Apr 15, 2020 10:30:00"' | ${CLICKHOUSE_LOCAL} --date_time_input_format basic --query "SELECT t FROM table" --structure "t DateTime('UTC')" --input-format CSV 2>&1 | grep -o -m1 -E 'Cannot parse|CANNOT_PARSE'

echo '-- TSV: best_effort by default parses a non-basic datetime'
printf '%s\n' '2024 April 4' | ${CLICKHOUSE_LOCAL} --query "SELECT t FROM table" --structure "t DateTime('UTC')" --input-format TSV

echo '-- TSV: basic mode rejects the same non-basic datetime'
printf '%s\n' '2024 April 4' | ${CLICKHOUSE_LOCAL} --date_time_input_format basic --query "SELECT t FROM table" --structure "t DateTime('UTC')" --input-format TSV 2>&1 | grep -o -m1 -E 'Cannot parse|CANNOT_PARSE'
