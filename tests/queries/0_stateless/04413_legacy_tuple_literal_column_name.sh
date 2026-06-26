#!/usr/bin/env bash
# Tags: no-debug, no-sanitizers
# With legacy_column_name_of_tuple_literal=1 the old analyzer derives the column name of a
# tuple literal through a dedicated recursive Field visitor (FieldVisitorToColumnName). For a
# deeply nested tuple literal built with a raised max_parser_depth this used to overflow the
# native stack while deriving the column name, before reaching type inference. It must report
# TOO_DEEP_RECURSION (code 306), never crash. Building such a literal is quadratic, so this is
# only fast enough in optimized builds; the stack guard it exercises is build-independent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Nested tuple of the form ((((1, 2), 2), 2), ...): every level has two elements, so it is
# unambiguously a tuple literal (one ASTLiteral whose value is a deeply nested Tuple Field).
python3 -c "
s = '(1, 2)'
for _ in range(35000 - 1):
    s = '(' + s + ', 2)'
print('SELECT ' + s + ' SETTINGS enable_analyzer = 0, legacy_column_name_of_tuple_literal = 1')" \
    | $CLICKHOUSE_CLIENT --max_parser_depth=100000000 --max_query_size=1000000000 2>&1 \
    | grep -oE "Code: [0-9]+" | head -1
