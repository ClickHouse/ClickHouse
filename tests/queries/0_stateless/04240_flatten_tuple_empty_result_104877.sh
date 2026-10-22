#!/usr/bin/env bash
# Regression test for `flattenTuple` LOGICAL_ERROR on empty-flatten inputs.
#
# `flattenTuple` recursively expands nested tuples and arrays into the
# underlying scalar columns. When the input type tree contains only
# empty `Tuple()` leaves (directly, or wrapped in any combination of
# `Tuple` / `Array`), the recursion produces zero scalar columns and
# the result reaches `ColumnTuple::create` with an empty column list.
# `ColumnTuple::create` raises `LOGICAL_ERROR` (Code 49) for empty
# input; under `abort_on_logical_error = true` (the default in CI
# test environments) this aborts the server.
#
# The fix detects the empty-flatten case in
# `FunctionFlattenTuple::getReturnTypeImpl` and throws
# `ILLEGAL_TYPE_OF_ARGUMENT` (Code 43) before reaching the
# `ColumnTuple::create` guard.
#
# This `.sh` form runs the bug-triggering queries inside
# `clickhouse-local` subprocesses so the abort on master HEAD stays
# contained. The bugfix-validation framework needs an output-diff
# `FAIL` on master HEAD (which it then inverts to `OK`); a server-side
# abort inside the parent server is classified as `ERROR` /
# `SERVER_DIED` and is not invertible. See PR #104861 for an earlier
# example of this `.sh` wrapper pattern for a contained-abort
# regression test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Prints `OK` if `clickhouse-local` exited cleanly with
# `ILLEGAL_TYPE_OF_ARGUMENT` (Code 43) — the PR-fix behavior.
# Prints `BUG` for any other outcome: SIGABRT under
# `abort_on_logical_error`, an unrelated error, or an unexpected
# success.
check_query()
{
    local out
    out=$($CLICKHOUSE_LOCAL --send_logs_level=fatal "$@" 2>&1 < /dev/null)
    local rc=$?
    if [[ $rc -eq 43 ]] || echo "$out" | grep -q "ILLEGAL_TYPE_OF_ARGUMENT"
    then
        echo "OK"
    else
        echo "BUG"
    fi
}

# Single-member tuple wrapping `Array(Tuple())` — the original
# fuzzer-reduced shape from #100272.
check_query --query "SELECT flattenTuple(CAST((tuple([])) AS Tuple(c0 Array(Tuple()))))"

# Single-member tuple wrapping a nested empty `Tuple()`.
check_query --query "SELECT flattenTuple(CAST((tuple(tuple())) AS Tuple(c0 Tuple())))"

# Deeper nesting of empty tuples.
check_query --query "SELECT flattenTuple(CAST((tuple(tuple(tuple()))) AS Tuple(c0 Tuple(c00 Tuple()))))"

# Double-array of empty tuple.
check_query --query "SELECT flattenTuple(CAST((tuple([[tuple()]])) AS Tuple(c0 Array(Array(Tuple())))))"

# `Nullable` wrappers around the same empty-result shapes reach the
# same code path and must throw the same user-facing error.
check_query --allow_experimental_nullable_tuple_type=1 \
            --query "SELECT flattenTuple(CAST((tuple([])) AS Nullable(Tuple(c0 Array(Tuple())))))"
check_query --allow_experimental_nullable_tuple_type=1 \
            --query "SELECT flattenTuple(CAST(NULL AS Nullable(Tuple(c0 Array(Tuple())))))"
