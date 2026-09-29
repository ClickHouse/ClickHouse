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

# The decision is cached per header column, while a field is read in the order the file lists it, so
# `CSVWithNames` with a reordered header is the case where the two indices differ. Here the file
# starts with the `Int32` column while the header of the table starts with the `String` one: the
# whitespace has to be skipped for the former and kept for the latter, and looking the cache up by
# the position of the field in the file rather than in the header would get both of them wrong.
echo '--- trim=0 CSVWithNames, header reordered'
printf 'n,s\n 42, padded \n' \
    | $CLICKHOUSE_LOCAL -S 's Nullable(String), n Int32' \
        --input-format=CSVWithNames --input_format_csv_trim_whitespaces=0 \
        -q "SELECT n, toString(s) FROM table FORMAT CSV"
