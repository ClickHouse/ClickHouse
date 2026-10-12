#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# When `input_format_csv_allow_variable_number_of_columns` (and the equivalent for `TSV`) is enabled, the
# parser accepts rows with fewer columns than the header, default-filling the rest. The schema-mismatch
# diagnostic's own inference must honor the same setting for the `*WithNames*` formats — the `CSVSchemaReader`
# / `TabSeparatedSchemaReader` (which serve the base, `WithNames` and `WithNamesAndTypes` variants) override
# `allowVariableNumberOfColumns` — so that a shorter data row does not make inference throw and silently drop
# the explanation for a genuine type mismatch elsewhere in the data.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CSVWithNames with a variable number of columns: a genuine text-into-numeric mismatch is still explained"
printf 'CREATE TABLE t (a String, b String, c UInt8) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_csv_allow_variable_number_of_columns = 1 FORMAT CSVWithNames\na,b,c\nfoo,bar,notanumber\nx,y\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSVWithNames with a variable number of columns: a genuine text-into-numeric mismatch is still explained"
printf 'CREATE TABLE t (a String, b String, c UInt8) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_tsv_allow_variable_number_of_columns = 1 FORMAT TSVWithNames\na\tb\tc\nfoo\tbar\tnotanumber\nx\ty\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
