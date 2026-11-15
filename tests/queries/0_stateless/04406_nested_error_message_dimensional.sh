#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The error message for the `nested` function used to interleave the dimension and the function name
# (e.g. "for function 2 must be nested-dimentional array") and misspelled "dimensional". Verify the
# message now substitutes the arguments in the right order and is spelled correctly.
$CLICKHOUSE_CLIENT --query "SELECT nested([['a']], [1, 2, 3])" 2>&1 | grep -o -m1 "must be 2-dimensional array"
