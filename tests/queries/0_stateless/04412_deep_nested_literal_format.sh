#!/usr/bin/env bash
# Tags: no-fasttest
#       no-fasttest: test pipes a 40KB query string through stdin and uses very high 
#       max_parser_depth, which is awkward in fasttest's constrained environment
# A deeply nested literal lives inside a single ASTLiteral::value, and formatting/hashing it
# walks that nested Field through recursive Field visitors. Reaching the format path without
# type inference (formatQuery) used to overflow the native stack and crash the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# formatQuery parses and re-formats the inner query without analysing it, so the deeply nested
# array literal is formatted through FieldVisitorToString. It must report TOO_DEEP_RECURSION
# (code 306), never crash. Piped through stdin because the query is too long for an argument.
python3 -c "print('SELECT formatQuery(\$\$SELECT ' + '['*20000 + '1' + ']'*20000 + '\$\$)')" \
    | $CLICKHOUSE_CLIENT --max_parser_depth=100000000 --max_query_size=1000000000 2>&1 \
    | grep -oE "Code: [0-9]+" | head -1
