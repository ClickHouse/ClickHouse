#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# On the `INSERT ... FROM INFILE` path the number of rows the parser reached is taken from the parse
# error message. `IInputFormat::generate` appends `(in file/uri <path>)` to the message after the
# parser's own row marker, and the file name is chosen by the user, so a name such as
# `data at row 50.tsv` must not be able to spoof the bound: everything starting from that suffix has
# to be ignored when looking for the marker. Otherwise schema inference samples rows the parser never
# reached, which both invents and suppresses the explanation of the parse error.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/04700_data at row 50_${CLICKHOUSE_DATABASE}.tsv"

# The parser fails on the first row; the second row would change the inferred type of the second
# column to `String` if it were sampled, inventing a mismatch with the destination.
printf '1\t1.5\n2\ttext\n' > "$DATA_FILE"

echo "-- the fake marker in the file name does not extend the sampled range (no false positive)"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE test_spoofed_file_name (a UInt8, b UInt8) ENGINE = Memory;
    INSERT INTO test_spoofed_file_name FROM INFILE '${DATA_FILE}' FORMAT TSV;
" < /dev/null 2>&1 | check

# The failing row itself does not match the destination: ignoring the file name suffix must not lose
# the genuine explanation.
printf 'text\thello\n' > "$DATA_FILE"

echo "-- a genuine mismatch is still explained with the fake marker in the file name"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE test_spoofed_file_name (a UInt8, b String) ENGINE = Memory;
    INSERT INTO test_spoofed_file_name FROM INFILE '${DATA_FILE}' FORMAT TSV;
" < /dev/null 2>&1 | check

rm -f "$DATA_FILE"
