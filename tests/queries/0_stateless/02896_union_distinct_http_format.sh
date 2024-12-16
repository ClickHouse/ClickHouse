#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

curl -d@- -sS "${CLICKHOUSE_URL}" <<< 'SELECT 1 UNION DISTINCT SELECT 1 SETTINGS output_format_pretty_color=1 FORMAT PrettyCompactMonoBlock'
curl -d@- -sS "${CLICKHOUSE_URL}" <<< 'SELECT * FROM (SELECT 1 as a UNION DISTINCT SELECT 2 as a) ORDER BY a SETTINGS output_format_pretty_color=1 FORMAT PrettyCompactMonoBlock'
