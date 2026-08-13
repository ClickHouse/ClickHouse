#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$CUR_DIR/scripts_udf

# An executable UDF is resolved through UserDefinedExecutableFunctionFactory, not FunctionFactory,
# so a function node the PREWHERE column restoration re-resolves cannot be rebuilt and the whole
# query is refused with UNKNOWN_FUNCTION. Nothing is replaced in any of these expressions, so the
# restoration must leave them alone whatever the surrounding shape is.

DATA="
    SET enable_analyzer = 1;
    CREATE TABLE lam_udf_l (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE lam_udf_r (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO lam_udf_l SELECT 1, 10;
    INSERT INTO lam_udf_r SELECT 1, 100;
"

run() {
    echo "$1"
    $CLICKHOUSE_LOCAL --multiquery -q "$DATA $2" \
        -- "--user_scripts_path=$SCRIPTS_DIR" \
           "--user_defined_executable_functions_config=$SCRIPTS_DIR/function.xml" < /dev/null
}

run 'udf outside a lambda' "
    SELECT id FROM lam_udf_l PREWHERE (test_function() = 'qwerty') AND (v > 0) ORDER BY id;"

run 'udf inside a lambda body' "
    SELECT id FROM lam_udf_l
    PREWHERE (arrayMap(z -> test_function(), [1])[1]) = 'qwerty' ORDER BY id;"

run 'udf inside a lambda body, nested' "
    SELECT id FROM lam_udf_l
    PREWHERE arrayExists(w -> (arrayMap(z -> test_function(), [1])[1]) = 'qwerty', [1]) ORDER BY id;"

# A restored join column in the same PREWHERE re-resolves its own subtree. The udf sibling shares
# no node with it, so it must stay untouched.
run 'udf beside a restored join column, join_use_nulls' "
    SELECT a.id FROM lam_udf_l AS a LEFT JOIN lam_udf_r AS b ON a.id = b.x
    PREWHERE (b.y != 0) AND ((arrayMap(z -> test_function(), [1])[1]) = 'qwerty')
    ORDER BY a.id SETTINGS join_use_nulls = 1;"

run 'udf before a restored join column, join_use_nulls' "
    SELECT a.id FROM lam_udf_l AS a LEFT JOIN lam_udf_r AS b ON a.id = b.x
    PREWHERE ((arrayMap(z -> test_function(), [1])[1]) = 'qwerty') AND (b.y != 0)
    ORDER BY a.id SETTINGS join_use_nulls = 1;"

# Here the lambda's own parameter is the restored column, so its body IS retyped. The udf reads no
# parameter, so it must still be left alone.
run 'udf in a lambda whose parameter is a restored join column, join_use_nulls' "
    SELECT a.id FROM lam_udf_l AS a LEFT JOIN lam_udf_r AS b ON a.id = b.x
    PREWHERE arrayExists(x -> (test_function() = 'qwerty') AND (x != 0), [b.y])
    ORDER BY a.id SETTINGS join_use_nulls = 1;"

run 'udf in a retyped lambda body, restored column also read outside' "
    SELECT a.id FROM lam_udf_l AS a LEFT JOIN lam_udf_r AS b ON a.id = b.x
    PREWHERE arrayExists(x -> (test_function() = 'qwerty') AND (x != 0), [b.y]) AND (b.y > 0)
    ORDER BY a.id SETTINGS join_use_nulls = 1;"
