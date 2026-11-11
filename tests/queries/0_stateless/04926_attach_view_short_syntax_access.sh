#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the short ATTACH VIEW is rejected in a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user04926_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.tbl (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.tbl VALUES (7);
DETACH TABLE $db.tbl;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT, CREATE VIEW ON $db.* TO $user;
EOF

${CLICKHOUSE_CLIENT} --user "$user" --query "ATTACH TABLE $db.tbl" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --user "$user" --query "ATTACH VIEW $db.tbl" 2>&1 | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --user "$user" --query "ATTACH MATERIALIZED VIEW $db.tbl" 2>&1 | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"

${CLICKHOUSE_CLIENT} <<EOF
SELECT count() FROM system.tables WHERE database = '$db' AND name = 'tbl';
ATTACH TABLE $db.tbl;
SELECT * FROM $db.tbl;
DROP TABLE $db.tbl;
DROP USER $user;
EOF
