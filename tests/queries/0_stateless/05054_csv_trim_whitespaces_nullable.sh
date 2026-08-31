#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `input_format_csv_trim_whitespaces = 0` keeps the whitespace that surrounds an unquoted CSV field
# when the column is a String or a FixedString. 02764_csv_trim_whitespaces covers the plain types;
# this covers their Nullable form, and a NULL next to whitespace, for both values of the setting.

for trim in 0 1
do
    for type in 'Nullable(String)' 'Nullable(FixedString(24))'
    do
        echo "--- trim=${trim} ${type}"
        printf ' padded ,\\N, \\N ,unpadded\n' \
            | $CLICKHOUSE_LOCAL -S "c1 ${type}, c2 ${type}, c3 ${type}, c4 ${type}" \
                --input-format=CSV --input_format_csv_trim_whitespaces=${trim} \
                -q "SELECT toString(c1), toString(c2), toString(c3), toString(c4) FROM table FORMAT CSV"
    done
done
