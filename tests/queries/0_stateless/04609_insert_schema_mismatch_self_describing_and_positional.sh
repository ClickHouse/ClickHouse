#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The schema-mismatch explanation must follow the real parser's own semantics, which depend on the
# format: self-describing formats (the -WithNamesAndTypes family) carry the declared column types in
# the data and the parser checks them against the destination exactly, while a -WithNames format read
# with `input_format_with_names_use_header = 0` ignores the file's names and maps columns positionally.
# In both cases a loose, name-based comparison could hide a genuine structure mismatch.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CSVWithNamesAndTypes: declared type differs from the destination (exact type check applies)"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes\n"x"\n"UInt64"\n5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- CSVWithNamesAndTypes: declared type matches the destination, only a value fails (no false positive)"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes\n"x"\n"UInt8"\n1.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- CSVWithNames, input_format_with_names_use_header=0: parser maps positionally, reordered header hides the mismatch"
printf 'CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNames\nb,a\nhello,1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_with_names_use_header 0 2>&1 | check

echo "-- CSVWithNames, input_format_with_names_use_header=1 (default): parser maps by name, structure matches (no false positive)"
printf 'CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNames\nb,a\nhello,1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_with_names_use_header 1 2>&1 | check
