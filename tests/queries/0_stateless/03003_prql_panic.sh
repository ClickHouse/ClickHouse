#!/usr/bin/env bash
# Tags: no-fasttest
# Requires Rust, which is not built for Fast Test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Before [1] this causes a panic, but it will be fixed soon, so do not check
# for panic, but just for SYNTAX_ERROR.
#
#   [1]: https://github.com/PRQL/prql/pull/4285
$CLICKHOUSE_CLIENT --dialect prql -q "SELECT id FROM distributed_test_table GROUP BY x -> concat(concat(materialize(toNullable(NULL)))) LIMIT 3" |& grep -o -m1 SYNTAX_ERROR
