#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on AWS

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

other_user="user03667_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS other_user;
CREATE USER $other_user;
GRANT SELECT ON $db.* TO $other_user;
EOF

${CLICKHOUSE_CLIENT} <<EOF
CREATE VIEW $db.test_view
SQL SECURITY DEFINER
AS SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/a.tsv');
EOF

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.test_view"
${CLICKHOUSE_CLIENT} --user $other_user --query "SELECT count() FROM $db.test_view"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.test_view SETTINGS enable_analyzer=0"
${CLICKHOUSE_CLIENT} --user $other_user --query "SELECT count() FROM $db.test_view SETTINGS enable_analyzer=0"

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW IF EXISTS $db.test_view;
DROP USER IF EXISTS $other_user;
EOF
