#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user03801_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.test_table (s String) ENGINE = MergeTree ORDER BY s;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE ON *.* TO $user;
EOF

${CLICKHOUSE_CLIENT} --user $user --query "
  CREATE VIEW $db.test_mv
  SQL SECURITY NONE
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"

${CLICKHOUSE_CLIENT} --user $user --query "
  ATTACH VIEW $db.test_mv UUID '8025ef9c-d735-4c16-ab4c-7f1f5110d049'
  (s String) SQL SECURITY NONE
  AS SELECT * FROM $db.test_table;
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"

${CLICKHOUSE_CLIENT} --query "GRANT ALLOW SQL SECURITY NONE ON *.* TO $user;"

${CLICKHOUSE_CLIENT} --user $user --query "
  ATTACH VIEW $db.test_mv UUID '7025ef9c-d735-4c16-ab4c-7f1f5110d049'
  (s String) SQL SECURITY NONE
  AS SELECT * FROM $db.test_table
  SETTINGS send_logs_level = 'error';
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.test_mv;"
${CLICKHOUSE_CLIENT} --query "DROP USER $user;"
