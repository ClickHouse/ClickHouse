#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

curl -d@- -sS "${CLICKHOUSE_URL}" <<< 'SELECT 1 as a UNION DISTINCT SELECT 2 as a order by a FORMAT PrettyCompactMonoBlock'
