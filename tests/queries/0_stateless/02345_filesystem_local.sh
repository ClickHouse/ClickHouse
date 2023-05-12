#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Checks that these functions are working inside clickhouse-local. Does not check specific values.
$CLICKHOUSE_LOCAL --query "SELECT filesystemAvailable() > 0, filesystemFree() <= filesystemCapacity()"
