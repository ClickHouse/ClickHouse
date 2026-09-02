#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS \`01818_with_names\`;"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`01818_with_names\` (t String) ENGINE = MergeTree ORDER BY t;"

echo -ne "t\ntestdata1\ntestdata2" | ${CLICKHOUSE_CLIENT} --input_format_with_names_use_header 0 --query "INSERT INTO \`01818_with_names\` FORMAT CSVWithNames"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM \`01818_with_names\`;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS \`01818_with_names\`;"
