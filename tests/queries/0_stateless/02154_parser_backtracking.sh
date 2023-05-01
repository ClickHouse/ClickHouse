#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Should finish in reasonable time (milliseconds).
# In previous versions this query led to exponential backtracking.

echo 'SELECT '"$(perl -e 'print "CAST(" x 100')"'a b c'"$(perl -e 'print ")" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(perl -e 'print "CAST(" x 100')"'a, b'"$(perl -e 'print ")" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(perl -e 'print "CAST(" x 100')"'a AS b'"$(perl -e 'print ")" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(perl -e 'print "CAST(" x 100')"'1'"$(perl -e 'print ", '"'UInt8'"')" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000
echo 'SELECT '"$(perl -e 'print "CAST(" x 100')"'1'"$(perl -e 'print " AS UInt8)" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000
echo "SELECT fo,22222?LUTAY(SELECT(NOT CAUTAY(SELECT(NOT CAST(NOTT(NOT CAST(NOT NOT LEfT(NOT coARRAYlumnsFLuTAY(SELECT(NO0?LUTAY(SELECT(NOT CAUTAY(SELECT(NOT CAST(NOTT(NOT CAST(NOT NOT LEfT(NOT coARRAYlumnsFLuTAY(SELECT(NOTAYTAY(SELECT(NOTAYEFAULT(fo,22222?LUTAY(%SELECT(NOT CAST(NOT NOTAYTAY(SELECT(NOTAYEFAULT(fo,22222?LUTAY(SELECT(NOT CAST(NOT NOT (NOe)))))))))))))))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'

echo "SELECT position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(a b))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo "SELECT position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(a, b))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo "SELECT position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(a, b, c))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'

echo 'SELECT '"$(perl -e 'print "position(" x 100')"'x'"$(perl -e 'print ")" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(perl -e 'print "position(" x 100')"'x y'"$(perl -e 'print ")" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(perl -e 'print "position(" x 100')"'x IN y'"$(perl -e 'print ")" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(perl -e 'print "position(" x 100')"'x'"$(perl -e 'print " IN x)" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'UNKNOWN_IDENTIFIER'
echo 'SELECT '"$(perl -e 'print "position(" x 100')"'x'"$(perl -e 'print ", x)" x 100')" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'UNKNOWN_IDENTIFIER'
