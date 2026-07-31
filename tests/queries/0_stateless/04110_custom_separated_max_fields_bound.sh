#!/usr/bin/env bash
# Regression test for MSan STID 4260-52c8:
# `CustomSeparatedFormatReader::readRowImpl` accumulated ~250 million fields
# into a `std::vector<String>` while reading an adversarial input in which
# `format_custom_row_after_delimiter` was never matched, requesting ~12 GiB
# before MSan's 8 GiB allocation cap fired. The fix caps the per-row field
# count at a hard-coded `MAX_FIELDS_PER_ROW = 1'000'000` inside
# `readRowImpl`, throwing `INCORRECT_DATA` once the cap is reached.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. Malformed input: 1_000_001 `-`-separated fields and no
#    `format_custom_row_after_delimiter`. `readRowForHeaderDetection`
#    -> `readRowImpl<AS_POSSIBLE_STRING>` must fail with `INCORRECT_DATA`
#    once `values.size()` reaches the 1_000_000 cap, instead of growing the
#    backing array unboundedly. We only need to grep for the error preamble:
#    the second test below covers the happy path explicitly.
echo "bounded:"
python3 -c 'import sys; sys.stdout.write("a"+("-a"*1000001))' \
    | $CLICKHOUSE_LOCAL \
        --input-format='CustomSeparated' --structure='x String' --table='t' \
        --format_custom_field_delimiter='-' \
        --format_custom_escaping_rule='CSV' \
        --input_format_custom_detect_header=1 \
        -q "select count() from t" 2>&1 \
    | grep -o 'Too many fields in a single row of CustomSeparated input' | head -n 1

# 2. Well-formed input is unaffected by the cap.
echo "well-formed:"
$CLICKHOUSE_LOCAL \
    --input-format='CustomSeparated' --structure='x String, y String, z String' --table='t' \
    --format_custom_field_delimiter='-' \
    --format_custom_escaping_rule='CSV' \
    --input_format_custom_detect_header=0 \
    -q "select count() from t" <<< $'a-b-c\nd-e-f\ng-h-i'
