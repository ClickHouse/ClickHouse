#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A non-String argument is rejected by argument validation before any dictionary is resolved, and a
# dictionary name that does not resolve is a bad argument.
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('sentiment', 3)" 2>&1 | grep -om1 'ILLEGAL_TYPE_OF_ARGUMENT'
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier(0, 'hello')" 2>&1 | grep -om1 'ILLEGAL_TYPE_OF_ARGUMENT'
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('zzz_nonexistent_model_4ae239f8', 'hello')" 2>&1 | grep -om1 'BAD_ARGUMENTS'
