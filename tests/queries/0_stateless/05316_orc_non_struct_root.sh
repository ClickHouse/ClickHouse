#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading an ORC file whose root type is not a struct must fail with an error instead of crashing.
# The fixtures have the root types `bigint` and `array<bigint>`.
BIGINT_ROOT="$CUR_DIR/data_orc/non_struct_root_bigint.orc"
ARRAY_ROOT="$CUR_DIR/data_orc/non_struct_root_array.orc"

check()
{
    local query="$1" expected="$2"
    local out
    out=$($CLICKHOUSE_LOCAL --query "$query" 2>&1)
    if [ -z "$out" ]; then
        echo "no output at all"
    else
        echo "$out" | grep -o -m1 "$expected" || echo "unexpected: $out"
    fi
}

check "SELECT * FROM file('$BIGINT_ROOT', ORC, 'x Int64') FORMAT Null" 'INCORRECT_DATA'
check "SELECT count() FROM file('$BIGINT_ROOT', ORC, 'x Int64')" 'INCORRECT_DATA'
check "DESCRIBE file('$ARRAY_ROOT', ORC)" 'root type of an ORC file must be a struct'
check "SELECT * FROM file('$ARRAY_ROOT', ORC, 'x Array(Int64)') FORMAT Null" 'INCORRECT_DATA'

# The message names the root type of the file.
check "SELECT * FROM file('$BIGINT_ROOT', ORC, 'x Int64') FORMAT Null" 'but it is bigint'

# A regular ORC file, whose root type is a struct, is still read.
STRUCT_ROOT="$CLICKHOUSE_TMP/05316_struct_root_${CLICKHOUSE_DATABASE}.orc"
rm -f "$STRUCT_ROOT"
$CLICKHOUSE_LOCAL --query "INSERT INTO FUNCTION file('$STRUCT_ROOT', ORC) SELECT number AS x FROM numbers(5)"
$CLICKHOUSE_LOCAL --query "SELECT sum(x) FROM file('$STRUCT_ROOT', ORC)"
rm -f "$STRUCT_ROOT"
