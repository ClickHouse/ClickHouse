#!/usr/bin/env bash
# A deeply nested array literal parses into a single ASTLiteral whose Field nests one Array per
# bracket. Analysing it clones the AST (deep-copying that Field) and eventually tears the Field
# down; both the copy and the destructor used to recurse once per nesting level and overflow the
# native stack. They are now iterative, so an arbitrarily deep literal must be rejected cleanly
# with TOO_DEEP_RECURSION by a stack-guarded walk, never crash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 -c "print('SELECT ' + '['*100000 + '1' + ']'*100000 + '; -- { serverError TOO_DEEP_RECURSION }')" \
    | $CLICKHOUSE_LOCAL --max_parser_depth=1000000000 --max_query_size=1000000000
