#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_03822_${CLICKHOUSE_DATABASE}_$RANDOM"
missing_txt="missing_03822_${CLICKHOUSE_DATABASE}_$RANDOM.txt"
missing_csv="missing_03822_${CLICKHOUSE_DATABASE}_$RANDOM.csv"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
EOF

(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT file('$missing_txt')" 2>&1 | grep -c "ACCESS_DENIED") >= 1 )) && echo "ACCESS_DENIED" || echo "UNEXPECTED";
(( $(${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE TABLE file('$missing_csv', 'CSV')" 2>&1 | grep -c "ACCESS_DENIED") >= 1 )) && echo "ACCESS_DENIED" || echo "UNEXPECTED";

${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO $user";

(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT file('$missing_txt')" 2>&1 | grep -c "FILE_DOESNT_EXIST") >= 1 )) && echo "FILE_DOESNT_EXIST" || echo "UNEXPECTED";
(( $(${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE TABLE file('$missing_csv', 'CSV')" 2>&1 | grep -c "CANNOT_STAT") >= 1 )) && echo "CANNOT_STAT" || echo "UNEXPECTED";

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user";
