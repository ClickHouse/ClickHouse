#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --param_foo |& grep -q -x 'Code: 36. DB::Exception: Parameter requires value'
$CLICKHOUSE_CLIENT --param_foo foo -q 'select {foo:String}'
$CLICKHOUSE_CLIENT -q 'select {foo:String}' --param_foo foo
