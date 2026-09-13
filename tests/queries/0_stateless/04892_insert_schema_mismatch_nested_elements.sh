#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The structure comparison of the parse-error diagnostic descends into the elements of an `Array` /
# `Tuple` / `Map` value. The value-level evidence must descend with it: a nested string that really
# holds text is a mismatch for a numeric element destination just like a top-level one, and a nested
# number other than `0` / `1` is a mismatch for a `Bool` element destination — while a nested quoted
# number (which the parser accepts into a numeric column) and a nested `0` / `1` must not be flagged.

# Case 1: a genuine text element for an `Array(UInt8)` destination is explained (the parse error
# itself comes from the value-level error in `n`).
echo '{"a":["oops"],"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Array(UInt8), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# Case 2: a quoted number is accepted into a numeric element, so it must not be explained.
echo '{"a":["1"],"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Array(UInt8), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# Case 3: a nested number other than `0` / `1` for an `Array(Bool)` destination is explained.
echo '{"a":[2],"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Array(Bool), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# Case 4: nested boolean literals are valid for an `Array(Bool)` destination.
echo '{"a":[1,0],"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Array(Bool), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# Case 5: the same for the elements of a `Tuple` destination read from an array token. (An array of
# values of different types infers as `Array(Dynamic)`, which stays deliberately inconclusive, so the
# token here is homogeneous.)
echo '{"a":["oops","text"],"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Tuple(UInt8, UInt8), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# Case 6: and for the values of a `Map` destination read from an object token.
echo '{"a":{"k":"oops"},"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Map(String, UInt8), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# Case 7: a valid `Map` value of the same shape is not flagged.
echo '{"a":{"k":"1"},"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Map(String, UInt8), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'The structure of the data being inserted'

# The parse error is reported in every case above.
echo '{"a":["oops"],"n":1.5}' | $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t_04892 (a Array(UInt8), n UInt8) ENGINE = Memory;
    INSERT INTO t_04892 FORMAT JSONEachRow
" 2>&1 | grep -c 'Cannot parse input'
