#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL}  --query "SELECT 'Table ' || database || '.' || name || ' does not have a comment' FROM system.tables WHERE name NOT LIKE '%\_log\_%' AND database='system' AND comment==''"
${CLICKHOUSE_CLIENT} --query "SELECT 'Table ' || database || '.' || name || ' does not have a comment' FROM system.tables WHERE name NOT LIKE '%\_log\_%' AND database='system' AND comment==''"
