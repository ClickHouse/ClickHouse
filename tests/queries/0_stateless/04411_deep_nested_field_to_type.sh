#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A large max_parser_depth lets the parser build a very deeply nested expression. The
# recursion that later walks it (building a DataType from a Field, converting a Field to a
# type, formatting/hashing/cloning the AST, masking secret arguments) must stay guarded so
# it reports TOO_DEEP_RECURSION (code 306) instead of overflowing the native stack and
# crashing the server. Matching any "Code: N" would also accept a server death surfacing as
# a connection-reset error, or an unrelated early limit, so the exact code is pinned.
#
# Depths: building a deeply nested literal Field is quadratic, so the array literal uses a
# modest depth (just deep enough to trip the guard in an optimized build, the worst case;
# sanitizer builds, with larger frames, trip it sooner). Nested function calls are linear to
# build and use a large depth. Queries go through stdin because they are too long for a
# command-line argument.
check() {
    local query="$1"
    echo "$query" \
        | $CLICKHOUSE_CLIENT --max_parser_depth=100000000 --max_query_size=1000000000 2>&1 \
        | grep -oE "Code: [0-9]+" | head -1
}

# Deeply nested array literal: FieldToDataType / getLeastSupertype (the type) and
# convertFieldToType (the constant column). Original report: toTypeName of such a literal.
check "SELECT $(python3 -c "print('['*2500 + '1' + ']'*2500)")"

# Deeply nested function calls during normal execution: the secret-argument masking walk
# (childrenHaveSecretParts) runs first and guards these.
check "SELECT $(python3 -c "print('array('*30000 + '1' + ')'*30000)")"
check "SELECT $(python3 -c "print('tuple('*30000 + '1' + ')'*30000)")"
check "SELECT 1$(python3 -c "print('+1'*30000)")"

# formatQuery parses and formats an AST without the secret-parts walk, so it directly
# exercises the AST formatting recursion guard (IAST::format).
check "SELECT formatQuery(\$\$SELECT $(python3 -c "print('array('*30000 + '1' + ')'*30000)")\$\$)"
