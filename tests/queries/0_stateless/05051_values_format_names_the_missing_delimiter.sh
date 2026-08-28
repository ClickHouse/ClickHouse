#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t (id UInt32, name String, d Date, e Enum8('a' = 1, 'b' = 2)) ORDER BY id
"

# The value itself parses; what is missing is the delimiter after it. This used to be reported as if the
# value were at fault - a row with fewer values than the table has columns blamed the last value it did
# read for not being parseable as its own (correct) type.
run()
{
    echo "--- $1"
    # `FORMAT Values` keeps reading data from stdin after the statement, so close it.
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t FORMAT Values $1" </dev/null 2>&1 | grep -m1 -E '^Code: ' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: Received from [^ ]* DB::Exception: /Code: \1. /' \
              -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While executing .*//' -e 's/ (version [^)]*)$//'
}

echo '=== a delimiter that is missing is reported as such'
run "(1, 'Alice')"
run "(1, 'Alice', '2020-01-02', 'a'"
run "(1 'Alice', '2020-01-02', 'a')"
run "(1, 'Alice', '2020-01-02', 'a', 'extra')"

echo
echo '=== a value that really cannot be parsed is still reported as such'
run "(1, 'Alice', '2020-01-02', notafunction(2))"
run "(*, 'Alice', '2020-01-02', 'a')"

echo
echo '=== valid rows, including expressions and several rows at once'
${CLICKHOUSE_CLIENT} -q "INSERT INTO t FORMAT Values (1, 'Alice', '2020-01-02', 'a'), (2, 'Bob', today() - 1, 'b')" </dev/null
${CLICKHOUSE_CLIENT} -q "SELECT id, name, e FROM t ORDER BY id"
