#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A large max_parser_depth lets the parser build a very deeply nested expression. The
# downstream recursion that walks it (building a DataType from a Field, converting a Field
# to a type, finding a common supertype, masking secret arguments in the AST) must stay
# guarded so it reports a clean exception instead of overflowing the native stack and
# crashing the server. Each query below must produce an exception, never a crash.

# Queries are piped through stdin because the deep ones are too long for a command-line argument.
check() {
    local query="$1"
    if echo "$query" \
        | $CLICKHOUSE_CLIENT --max_parser_depth=100000000 --max_query_size=1000000000 2>&1 \
        | grep -qE "Code: [0-9]+"
    then
        echo "exception"
    else
        echo "NO EXCEPTION (unexpected success or crash)"
    fi
}

# Deep Array/Tuple/Map literals: FieldToDataType / convertFieldToType / getLeastSupertype.
check "SELECT toTypeName($(python3 -c "print('['*5000 + '1' + ']'*5000)"))"
check "SELECT $(python3 -c "print('['*5000 + '1' + ']'*5000)")"
check "SELECT $(python3 -c "print('('*5000 + '1,2' + ')'*5000)")"

# Deep nested function calls: AST secret-argument masking (childrenHaveSecretParts).
check "SELECT $(python3 -c "print('array('*30000 + '1' + ')'*30000)")"
check "SELECT $(python3 -c "print('tuple('*30000 + '1' + ')'*30000)")"
check "SELECT 1$(python3 -c "print('+1'*30000)")"
