#!/usr/bin/env bash
# Regression coverage for the basic parser of small decimal unix timestamps:
#   1. the DateTimeFractionPrefix passthrough, exercised when a value is split across read buffers;
#   2. agreement between the throwing and non-throwing entrypoints (e.g. "-.5", "1234", "0").

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/small_decimal_dt64_${CLICKHOUSE_DATABASE}.tsv"
printf '1234.5\n3333.77\n23.9\n2025.08.31\n-0.1\n+.5\n+1234.5\n' > "$DATA_FILE"

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
    toDateTime64OrNull('+.5', 3, 'UTC') IS NOT NULL AND toDateTime64OrNull('+.5', 3, 'UTC') = toDateTime64('+.5', 3, 'UTC'),
    toDateTime64OrNull('+1234.5', 3, 'UTC') IS NOT NULL AND toDateTime64OrNull('+1234.5', 3, 'UTC') = toDateTime64('+1234.5', 3, 'UTC'),
    toDateTime64OrNull('-0.5', 3, 'UTC') = toDateTime64('-0.5', 3, 'UTC'),
    toDateTime64OrNull('1234', 3, 'UTC') = toDateTime64('1234', 3, 'UTC'),
    toDateTime64OrNull('0', 3, 'UTC')    = toDateTime64('0', 3, 'UTC')
"

# Leading-dot fractions long enough / zero-padded to reach the optimistic path (>= 19 bytes) must also
# agree between the throwing and non-throwing entrypoints (the optimistic path used to reject '.').
${CLICKHOUSE_LOCAL} -q "
SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';
SELECT
    toDateTime64OrNull('.1234567890123456789', 3, 'UTC') IS NOT NULL
        AND toDateTime64OrNull('.1234567890123456789', 3, 'UTC') = toDateTime64('.1234567890123456789', 3, 'UTC'),
    toDateTime64OrNull(toFixedString('.5', 20), 3, 'UTC') IS NOT NULL
        AND toDateTime64OrNull(toFixedString('.5', 20), 3, 'UTC') = toDateTime64(toFixedString('.5', 20), 3, 'UTC')
"

# Multi-column row format (optimistic path, >= 19 bytes): a 4-digit field must stop at the field
# delimiter, not read a tab as a date separator and consume the following columns. Plain DateTime
# rejects the 4-digit field in the same path.
DATA_MC="${CLICKHOUSE_TMP}/multicol_dt64_${CLICKHOUSE_DATABASE}.tsv"
printf '1234\t12\t30\t99999999\n' > "$DATA_MC"
${CLICKHOUSE_LOCAL} -q "SELECT a = toDateTime64('1234', 2, 'UTC') AND b = 12 AND c = 30 AND d = 99999999 FROM file('${DATA_MC}', TSV, 'a DateTime64(2, \'UTC\'), b UInt32, c UInt32, d UInt32') SETTINGS date_time_input_format='basic', cast_string_to_date_time_mode='basic'"
rm -f "$DATA_MC"

# A dot with no fractional digit (\".\", \"+.\", \"-.\", \".x\") is not a valid DateTime64.
${CLICKHOUSE_LOCAL} -q "
SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';
SELECT
    toDateTime64OrNull('.', 3, 'UTC') IS NULL,
    toDateTime64OrNull('+.', 3, 'UTC') IS NULL,
    toDateTime64OrNull('-.', 3, 'UTC') IS NULL,
    toDateTime64OrNull('.x', 3, 'UTC') IS NULL,
    toDateTime64OrNull(toFixedString('.', 20), 3, 'UTC') IS NULL
"

# The throwing entrypoint must also reject a dot with no fractional digit, including zero-padded
# FixedString values long enough for the optimistic path (they used to be recovered as the epoch).
# The signed FixedString values fail in the integer part, so the error is CANNOT_PARSE_NUMBER there.
for value in "'.'" "'+.'" "'-.'" "toFixedString('.', 20)" "toFixedString('+.', 20)" "toFixedString('-.', 20)"
do
    ${CLICKHOUSE_LOCAL} -q "SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic'; SELECT toDateTime64(${value}, 3, 'UTC')" 2>&1 \
        | grep -qE 'CANNOT_PARSE_DATETIME|CANNOT_PARSE_NUMBER' && echo "rejected: ${value}" || echo "UNEXPECTED SUCCESS: ${value}"
done
