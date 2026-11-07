#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS snapshot_dist;
        DROP TABLE IF EXISTS snapshot_base;
    "
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE snapshot_base (id UInt64) ENGINE = Memory;
    INSERT INTO snapshot_base VALUES (0), (1), (2), (3);
    CREATE TABLE snapshot_dist AS snapshot_base
        ENGINE = Distributed(test_shard_localhost, currentDatabase(), snapshot_base);
"

# The remote child appends a source-table filter without changing the session's `Map`.
# Disable local execution and plan serialization to exercise the settings-packet path.
${CLICKHOUSE_CLIENT} --multiquery --query "
    SET prefer_localhost_replica = 0, serialize_query_plan = 0;
    SET additional_table_filters = {'snapshot_dist': 'id >= 2'};
    SELECT sum(id) FROM snapshot_dist;
    SELECT mapContains(getSetting('additional_table_filters'), 'snapshot_base');
    SELECT sum(id) FROM snapshot_dist SETTINGS additional_table_filters = {'snapshot_dist': 'id >= 3'};
    SELECT sum(id) FROM snapshot_dist;
    SELECT sum(id) FROM snapshot_base;
    SET compatibility = '22.8';
    SELECT sum(id) FROM snapshot_dist;
    SELECT mapContains(getSetting('additional_table_filters'), 'snapshot_base');
    SET compatibility = '';
    SELECT sum(id) FROM snapshot_dist;
"
