#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON RandomName34343 TO user_${CLICKHOUSE_DATABASE}" 2>&1 | grep -om1 "Unknown table engine"
