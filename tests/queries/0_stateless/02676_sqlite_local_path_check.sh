#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "SELECT * FROM sqlite('/nonexistent', 'table')" 2>&1 | grep -c "PATH_ACCESS_DENIED";
