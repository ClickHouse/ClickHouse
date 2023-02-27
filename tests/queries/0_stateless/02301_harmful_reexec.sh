#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: we can do a better test with strace, but I don't think that it is worth it.
$CLICKHOUSE_LOCAL -q "SELECT 1"
LD_LIBRARY_PATH=/tmp $CLICKHOUSE_LOCAL -q "SELECT 1"
