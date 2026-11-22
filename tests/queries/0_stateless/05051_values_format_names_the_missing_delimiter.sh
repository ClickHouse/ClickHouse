#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMA="id UInt32, name String, d Date, n UInt8"

# The value itself parses; what is missing is the delimiter after it. This used to be reported as if the
# value were at fault - a row with fewer values than the table has columns blamed the last value it did
# read for not being parseable as its own (correct) type.
#
# `format` runs the same reader as an INSERT does, without a table and without a server, so nothing here
# depends on how an insert is executed.
run()
{
    echo "--- $1"
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM format(Values, '$SCHEMA', \$\$$1\$\$)" </dev/null 2>&1 \
        | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While executing .*//' -e 's/: In scope .*//' -e 's/ (version [^)]*)$//'
}

echo '=== a delimiter that is missing is reported as such'
run "(1, 'Alice')"
run "(1, 'Alice', '2020-01-02', 7"
run "(1 'Alice', '2020-01-02', 7)"
run "(1, 'Alice', '2020-01-02', 7, 8)"

echo
echo '=== a value that really cannot be parsed is still reported as such'
run "(1, 'Alice', '2020-01-02', notafunction(2))"
run "(*, 'Alice', '2020-01-02', 7)"

echo
echo '=== the optional trailing comma before ) is accepted for an expression too, as it is for a literal'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM format(Values, '$SCHEMA', \$\$(1, 'Alice', '2020-01-02', 7,)\$\$)" </dev/null
${CLICKHOUSE_LOCAL} --query "SELECT * FROM format(Values, '$SCHEMA', \$\$(2, 'Bob', toDate('2020-01-02') + 1, 3 + 5,)\$\$)" </dev/null

echo
echo '=== valid rows, including an expression and several rows at once'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM format(Values, '$SCHEMA', \$\$(1, 'Alice', '2020-01-02', 7), (2, 'Bob', toDate('2020-01-02') + 1, 3 + 5)\$\$) ORDER BY id" </dev/null
