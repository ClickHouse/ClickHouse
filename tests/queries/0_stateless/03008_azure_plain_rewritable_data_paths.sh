#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-distributed-cache
# Tag no-fasttest: requires Azure
# Tag no-shared-merge-tree: does not support replication
# Tag no-distributed-cache: Not supported auth type

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

container="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"
namespace="${container}/plain-tables"

${CLICKHOUSE_CLIENT} --query "drop table if exists 03008_azure_data_paths"

${CLICKHOUSE_CLIENT} -nm --query "
CREATE TABLE 03008_azure_data_paths (a Int32, b Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '${container}',
    path='/var/lib/clickhouse/disks/${container}/tables',
    container_name = '${container}',
    endpoint = 'http://localhost:10000/devstoreaccount1/${namespace}',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO 03008_azure_data_paths (*) SELECT number, number % 3 FROM numbers_mt(100);
"

${CLICKHOUSE_CLIENT} --query "
SELECT path FROM system.tables
ARRAY JOIN data_paths as path
WHERE database=currentDatabase() AND name='03008_azure_data_paths'
    AND NOT startsWith(path, '${namespace}/');
"

${CLICKHOUSE_CLIENT} --query "drop table 03008_azure_data_paths"
