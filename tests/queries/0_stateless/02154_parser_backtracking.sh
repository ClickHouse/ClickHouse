#!/usr/bin/env bash
# Tags: no-tsan
# ^ TSan uses more stack

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Should finish in reasonable time (milliseconds).
# In previous versions this query led to exponential backtracking.

echo 'SELECT '"$(rep 'CAST(' 100)"'a b c'"$(rep ')' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(rep 'CAST(' 100)"'a, b'"$(rep ')' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(rep 'CAST(' 100)"'a AS b'"$(rep ')' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(rep 'CAST(' 100)"'1'"$(rep ", 'UInt8')" 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000
echo 'SELECT '"$(rep 'CAST(' 100)"'1'"$(rep ' AS UInt8)' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000
echo "SELECT fo,22222?LUTAY(SELECT(NOT CAUTAY(SELECT(NOT CAST(NOTT(NOT CAST(NOT NOT LEfT(NOT coARRAYlumnsFLuTAY(SELECT(NO0?LUTAY(SELECT(NOT CAUTAY(SELECT(NOT CAST(NOTT(NOT CAST(NOT NOT LEfT(NOT coARRAYlumnsFLuTAY(SELECT(NOTAYTAY(SELECT(NOTAYEFAULT(fo,22222?LUTAY(%SELECT(NOT CAST(NOT NOTAYTAY(SELECT(NOTAYEFAULT(fo,22222?LUTAY(SELECT(NOT CAST(NOT NOT (NOe)))))))))))))))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'

echo "SELECT position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(a b))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo "SELECT position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(a, b))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo "SELECT position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(position(a, b, c))))))))))))))))))))" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'

echo 'SELECT '"$(rep 'position(' 100)"'x'"$(rep ')' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(rep 'position(' 100)"'x y'"$(rep ')' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(rep 'position(' 100)"'x IN y'"$(rep ')' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
echo 'SELECT '"$(rep 'position(' 100)"'x'"$(rep ' IN x)' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'UNKNOWN_IDENTIFIER'
echo 'SELECT '"$(rep 'position(' 100)"'x'"$(rep ', x)' 100)" | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'UNKNOWN_IDENTIFIER'
