#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `table_disk` the table occupies the root of the disk, which cannot be removed recursively, so `DROP TABLE` has to
# remove the state that the table keeps there itself. Otherwise the next table created on the same disk loads the
# deduplication log of the previous one and skips its inserts as duplicates.

endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
disk_args="type = s3_plain_rewritable, endpoint = '${endpoint}', access_key_id = clickhouse, secret_access_key = clickhouse"

function create_table()
{
    ${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE dedup (key Int32) ENGINE = MergeTree ORDER BY key
    SETTINGS table_disk = 1, disk = disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}', ${disk_args}),
             non_replicated_deduplication_window = 100;
    "
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dedup SYNC"

create_table
${CLICKHOUSE_CLIENT} --query "INSERT INTO dedup VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO dedup VALUES (1), (2), (3)"

echo '-- the repeated insert of the same block is deduplicated'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM dedup"

${CLICKHOUSE_CLIENT} --query "DROP TABLE dedup SYNC"

echo '-- the drop leaves no objects behind'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3('${endpoint}**', 'clickhouse', 'clickhouse', 'One')"

echo '-- a table created on the same disk afterwards does not inherit the deduplication log'
create_table
${CLICKHOUSE_CLIENT} --query "INSERT INTO dedup VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM dedup"

${CLICKHOUSE_CLIENT} --query "DROP TABLE dedup SYNC"
