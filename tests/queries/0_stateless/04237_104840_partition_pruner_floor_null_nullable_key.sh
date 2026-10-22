#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104840
#
# `KeyCondition::applyFunctionChainToColumn` used to call `assert_cast<const ColumnNullable &>`
# on the result of `func->execute` after stripping outer `LowCardinality` but NOT outer `Const`.
# For partition expressions where the chain collapses to a single constant value (e.g.
# `floor(NULL, x)` always returns NULL), the function returned `ColumnConst(ColumnNullable(...))`
# of size 1. `ColumnConst::isNullable` reports the wrapped column's nullability, so the
# `isNullable()` guard accepted the column while the subsequent `assert_cast` aborted with:
#
#     Bad cast from type DB::ColumnConst to DB::ColumnNullable
#
# (STID: 3520-4237 in the AST fuzzer infra)
#
# We run the bug-triggering scenario inside a `clickhouse-local` subprocess so the abort
# stays contained. The bugfix-validation framework needs an output-diff `FAIL` on master HEAD
# (which it then inverts to `OK`); a server-side crash is classified as `ERROR` / `SERVER_DIED`
# and is not invertible.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Suppress stack-trace dumps from the contained abort on master HEAD.
if $CLICKHOUSE_LOCAL --send_logs_level=fatal --query "
    CREATE TABLE t_104840
    (
        myDay Date NULL,
        myOrder DateTime('UTC')
    )
    ENGINE = MergeTree
    PARTITION BY floor(NULL, toRelativeYearNum(myDay))
    ORDER BY myOrder
    SETTINGS allow_nullable_key = 1, allow_suspicious_indices = 1;

    INSERT INTO t_104840 VALUES ('2021-01-02', 2);

    SELECT myDay, myOrder FROM t_104840 WHERE myDay = '2021-01-02' ORDER BY myOrder;
" > /dev/null 2>&1
then
    echo "OK"
else
    echo "BUG"
fi
