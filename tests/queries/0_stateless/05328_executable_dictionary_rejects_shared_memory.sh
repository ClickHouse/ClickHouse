#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The shared-memory transport exists only for executable user defined functions. A dictionary that
# asks for it has to fail, because the alternative is that it loads and runs over the pipes instead -
# a different transport from the one it was configured for, with nothing said about it. Executable
# dictionaries can be created with DDL only in `clickhouse-local`.
for source in EXECUTABLE EXECUTABLE_POOL; do
    output=$($CLICKHOUSE_LOCAL --query "
        CREATE DICTIONARY d (id UInt64, result String) PRIMARY KEY id
            SOURCE($source(COMMAND 'cat' FORMAT 'TabSeparated' USE_SHARED_MEMORY 1))
            LAYOUT(COMPLEX_KEY_DIRECT());
        SELECT dictGet('d', 'result', toUInt64(1));
    " 2>&1)
    if grep -q "use_shared_memory.*(UNSUPPORTED_METHOD)" <<< "$output"; then
        echo "$source OK"
    else
        echo "$source: $output"
    fi
done
