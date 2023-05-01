#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "SELECT count(), arrayJoin([1, 2, 3]) AS n GROUP BY n WITH TOTALS ORDER BY n LIMIT 1 FORMAT JSON;" 2>&1 | head -32

