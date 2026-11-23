#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user03702_${CLICKHOUSE_DATABASE}_$RANDOM"
role="role03702_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

SET distributed_ddl_output_mode = 'none';
DROP USER IF EXISTS $user ON CLUSTER test_cluster_two_shards_different_databases;
CREATE USER $user ON CLUSTER test_cluster_two_shards_different_databases;

DROP ROLE IF EXISTS $role ON CLUSTER test_cluster_two_shards_different_databases;
CREATE ROLE $role ON CLUSTER test_cluster_two_shards_different_databases;

GRANT REMOTE ON *.* TO $user ON CLUSTER test_cluster_two_shards_different_databases;
GRANT SELECT ON *.* TO $role ON CLUSTER test_cluster_two_shards_different_databases;

GRANT $role TO $user ON CLUSTER test_cluster_two_shards_different_databases;
DROP ROLE $role ON CLUSTER test_cluster_two_shards_different_databases;
EOF

${CLICKHOUSE_CLIENT} --user $user <<EOF
SELECT
    hostName() AS h,
    count()
FROM clusterAllReplicas('test_cluster_two_shards_different_databases', system.one)
GROUP BY h
FORMAT Null;
EOF

${CLICKHOUSE_CLIENT} <<EOF
SET distributed_ddl_output_mode = 'none';
DROP USER IF EXISTS $user ON CLUSTER test_cluster_two_shards_different_databases;
EOF
