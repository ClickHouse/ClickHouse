#!/usr/bin/env bash
# Regression coverage for the basic parser of small decimal unix timestamps:
#   1. the DateTimeFractionPrefix passthrough, exercised when a value is split across read buffers;
#   2. agreement between the throwing and non-throwing entrypoints (e.g. "-.5", "1234", "0").

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/small_decimal_dt64_${CLICKHOUSE_DATABASE}.tsv"
printf '1234.5\n3333.77\n23.9\n2025.08.31\n-0.1\n' > "$DATA_FILE"

# A 2-byte read buffer forces every value to be refilled mid-parse, so the dot and fraction
# digits consumed while probing for a YYYY-MM-DD date must be passed through, not dropped.
# The result must be identical to parsing the same file with a normal buffer.
QUERY="SELECT a FROM file('${DATA_FILE}', TSV, 'a DateTime64(2, \'UTC\')') ORDER BY a SETTINGS date_time_input_format='basic'"
split=$(${CLICKHOUSE_LOCAL} --max_read_buffer_size=2 --input_format_parallel_parsing=0 -q "$QUERY")
whole=$(${CLICKHOUSE_LOCAL} -q "$QUERY")
if [ "$split" = "$whole" ]; then echo "split parsing matches"; else echo "MISMATCH"; diff <(echo "$whole") <(echo "$split"); fi

rm -f "$DATA_FILE"

# The non-throwing entrypoints (OrNull / schema inference) must agree with the throwing ones,
# including leading-dot fractions with and without a sign (".5", "-.5") and bare small integers.
${CLICKHOUSE_LOCAL} -q "
SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';
SELECT
    toDateTime64OrNull('-.5', 3, 'UTC') IS NOT NULL AND toDateTime64OrNull('-.5', 3, 'UTC') = toDateTime64('-.5', 3, 'UTC'),
    toDateTime64OrNull('.5', 3, 'UTC')  IS NOT NULL AND toDateTime64OrNull('.5', 3, 'UTC')  = toDateTime64('.5', 3, 'UTC'),
    toDateTime64OrNull('-0.5', 3, 'UTC') = toDateTime64('-0.5', 3, 'UTC'),
    toDateTime64OrNull('1234', 3, 'UTC') = toDateTime64('1234', 3, 'UTC'),
    toDateTime64OrNull('0', 3, 'UTC')    = toDateTime64('0', 3, 'UTC')
"
