#!/usr/bin/env bash
# Tags: replica

# CREATE AS SELECT for Replicated database is broken (https://github.com/ClickHouse/ClickHouse/issues/35408).
# This should be fixed and this test should eventually be deleted.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db engine = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_db', '{shard}', '{replica}')"
# Non-replicated engines are allowed
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.test (id UInt64) ENGINE = MergeTree() ORDER BY id AS SELECT 1"
# Replicated storafes are forbidden
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.test2 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test2', '1') ORDER BY id AS SELECT 1" |& grep -cm1 "SUPPORT_IS_DISABLED"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_db"
