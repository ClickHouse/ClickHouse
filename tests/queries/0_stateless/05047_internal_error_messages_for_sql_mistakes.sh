#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Only the code and the message matter here, so drop the scope and the version from them.
run()
{
    echo "--- $1"
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -m1 -E '^Code: ' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: Received from [^ ]* DB::Exception: /Code: \1. /' \
              -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: In scope .*//' -e 's/ (version [^)]*)$//' \
              -e 's/Cannot parse date: [^:]*/Cannot parse date: <reason>/'
}

echo '=== a string that a number cannot be parsed from is reported the same way whether it is empty or not'
run "SELECT toInt32(1) = 'abc'"
run "SELECT toInt32(1) = '12abc'"
run "SELECT toInt32(1) = ''"
run "SELECT toUInt8(1) IN ('', '1')"
run "SELECT toDate('2020-01-01') = ''"

echo
echo '=== a correlated subquery of several columns where a single value is expected'
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS outer_table;
    DROP TABLE IF EXISTS inner_table;
    CREATE TABLE outer_table (a UInt32) ORDER BY a;
    CREATE TABLE inner_table (b UInt32, c UInt32) ORDER BY b;
"
# `NOT EXISTS (...)` that lost its `EXISTS`.
run "SELECT a FROM outer_table WHERE NOT (SELECT * FROM inner_table WHERE b = a)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE outer_table; DROP TABLE inner_table"
