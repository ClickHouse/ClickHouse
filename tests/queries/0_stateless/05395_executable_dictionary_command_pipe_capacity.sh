#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: `F_SETPIPE_SZ` is Linux-only, and `command_pipe_capacity` has no effect elsewhere

# `command_pipe_capacity` on the dictionary side: the pipes to the command of an `executable` and an
# `executable_pool` source are enlarged to it, as they are for an executable user defined function.
# The command reports the capacities it sees, for a source with the option and for the same source
# without it. Executable dictionaries can be created with DDL only in `clickhouse-local`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# Twice the default capacity of a new pipe, so that a pipe left alone is below it whatever the page
# size. A host that does not let a pipe grow that far - over `pipe-max-size`, or a user past
# `pipe-user-pages-soft` - cannot tell the two apart, so the test prints its reference and stops.
CAPACITY=$("$SHM_UDF_SCRIPTS/pipe_capacity.py" --probe)
if [ "$CAPACITY" = 0 ]; then
    cat "$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh).reference"
    exit 0
fi

echo -n | shm_functions

# Whether each of the pipes - `stdin`, `stdout`, `stderr` - is at least `CAPACITY` bytes.
ENLARGED="arrayMap(x -> toUInt64(x) >= $CAPACITY, splitByChar(' ', result))"

# A dictionary named `name` over `pipe_capacity.py`, with the source options given.
function source_dictionary()
{
    echo "CREATE DICTIONARY $1 (input UInt64, result String) PRIMARY KEY input
        SOURCE(EXECUTABLE(COMMAND 'pipe_capacity.py' FORMAT 'TabSeparated' EXECUTE_DIRECT 1 $2)) LAYOUT(FLAT()) LIFETIME(0);"
}

function pool_dictionary()
{
    echo "CREATE DICTIONARY $1 (input UInt64, result String) PRIMARY KEY input
        SOURCE(EXECUTABLE_POOL(COMMAND 'pipe_capacity.py --pool' FORMAT 'TabSeparated' EXECUTE_DIRECT 1 POOL_SIZE 1 $2)) LAYOUT(DIRECT());"
}

shm_local "
    $(source_dictionary source_default "")
    $(source_dictionary source_enlarged "COMMAND_PIPE_CAPACITY $CAPACITY")
    $(pool_dictionary pool_default "")
    $(pool_dictionary pool_enlarged "COMMAND_PIPE_CAPACITY $CAPACITY")
    SELECT 'executable, default', $ENLARGED FROM dictionary(source_default);
    SELECT 'executable, enlarged', $ENLARGED FROM dictionary(source_enlarged);
    SELECT 'executable_pool, default', $ENLARGED FROM (SELECT dictGet('pool_default', 'result', toUInt64(1)) AS result);
    SELECT 'executable_pool, enlarged', $ENLARGED FROM (SELECT dictGet('pool_enlarged', 'result', toUInt64(1)) AS result);
"
