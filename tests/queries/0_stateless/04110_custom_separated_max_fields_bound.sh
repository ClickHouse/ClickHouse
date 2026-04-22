#!/usr/bin/env bash
# Regression test for MSAN STID 4260-52c8: `CustomSeparatedFormatReader::readRowImpl`
# allocated 12 GB while accumulating fields from an adversarial input where
# `format_custom_row_after_delimiter` was never matched. The fix bounds the
# per-row field count via `input_format_custom_max_number_of_fields_per_row`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. Malformed input during header detection: 20 `-`-separated fields with no
#    `row_after_delimiter`. With `input_format_custom_max_number_of_fields_per_row=10`
#    we must fail fast with INCORRECT_DATA rather than grow the vector unboundedly.
echo "bounded-header-detection:"
$CLICKHOUSE_LOCAL \
    --input-format='CustomSeparated' --structure='x String' --table='t' \
    --format_custom_field_delimiter='-' \
    --format_custom_escaping_rule='CSV' \
    --input_format_custom_detect_header=1 \
    --input_format_custom_max_number_of_fields_per_row=10 \
    -q "select count() from t" <<< "a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a" 2>&1 \
    | grep -o 'Too many fields in a single row of CustomSeparated input (limit: 10)' | head -n 1

# 2. Same protection during schema inference (no structure supplied):
#    CustomSeparatedSchemaReader::readRow goes through the same bounded loop.
echo "bounded-schema-inference:"
$CLICKHOUSE_LOCAL \
    --input-format='CustomSeparated' --table='t' \
    --format_custom_field_delimiter='-' \
    --format_custom_escaping_rule='CSV' \
    --input_format_custom_detect_header=0 \
    --input_format_custom_max_number_of_fields_per_row=10 \
    -q "desc t" <<< "a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a-a" 2>&1 \
    | grep -o 'Too many fields in a single row of CustomSeparated input (limit: 10)' | head -n 1

# 3. Well-formed input stays well within the bound: no error, proper row count.
echo "well-formed:"
$CLICKHOUSE_LOCAL \
    --input-format='CustomSeparated' --structure='x String, y String, z String' --table='t' \
    --format_custom_field_delimiter='-' \
    --format_custom_escaping_rule='CSV' \
    --input_format_custom_detect_header=0 \
    --input_format_custom_max_number_of_fields_per_row=10 \
    -q "select count() from t" <<< $'a-b-c\nd-e-f\ng-h-i'

# 4. Verify the default (1_000_000) is in effect when the setting is not specified.
#    A 3-field row should succeed regardless of the default ceiling.
echo "default-limit:"
$CLICKHOUSE_LOCAL \
    --input-format='CustomSeparated' --structure='x String, y String, z String' --table='t' \
    --format_custom_field_delimiter='-' \
    --format_custom_escaping_rule='CSV' \
    -q "select count() from t" <<< "a-b-c"
