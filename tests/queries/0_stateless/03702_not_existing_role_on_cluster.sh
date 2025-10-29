#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user03702_${CLICKHOUSE_DATABASE}_$RANDOM"
role="role03702_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
SET distributed_ddl_output_mode = 'none';
DROP USER IF EXISTS $user ON CLUSTER test_cluster_interserver_secret;
CREATE USER $user ON CLUSTER test_cluster_interserver_secret;

DROP ROLE IF EXISTS $role ON CLUSTER test_cluster_interserver_secret;
CREATE ROLE $role ON CLUSTER test_cluster_interserver_secret;

GRANT REMOTE ON *.* TO $user ON CLUSTER test_cluster_interserver_secret;
GRANT SELECT ON *.* TO $role ON CLUSTER test_cluster_interserver_secret;

GRANT $role TO $user ON CLUSTER test_cluster_interserver_secret;
DROP ROLE $role ON CLUSTER test_cluster_interserver_secret;
EOF

${CLICKHOUSE_CLIENT} --user $user <<EOF
SELECT
    hostName() AS h,
    count()
FROM clusterAllReplicas('test_cluster_interserver_secret', system.one)
GROUP BY h
FORMAT Null;
EOF

${CLICKHOUSE_CLIENT} <<EOF
SET distributed_ddl_output_mode = 'none';
DROP USER IF EXISTS $user ON CLUSTER test_cluster_interserver_secret;
EOF
