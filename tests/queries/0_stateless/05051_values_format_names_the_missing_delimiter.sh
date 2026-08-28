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
#
# The insert goes over HTTP: the message is what matters here, and the HTTP interface returns it in one
# piece, without the client's framing and without depending on whether the insert is asynchronous.
run()
{
    echo "--- $1"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" \
        --data-binary "INSERT INTO t FORMAT Values $1" 2>&1 \
        | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While executing .*//' -e 's/: In scope .*//' -e 's/ (version [^)]*)$//'
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
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary "INSERT INTO t FORMAT Values (1, 'Alice', '2020-01-02', 'a'), (2, 'Bob', today() - 1, 'b')"
${CLICKHOUSE_CLIENT} -q "SELECT id, name, e FROM t ORDER BY id"
