#!/usr/bin/env bash
# Tags: no-fasttest, no-distributed-cache
# Tag no-fasttest: requires Azure
# Tag no-distributed-cache: Not supported auth type

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONTAINER="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"

DISK_NAME="$CONTAINER"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS test_azure_readonly;

    CREATE TABLE test_azure_readonly (key Int, arr Array(UInt32)) ENGINE=MergeTree() ORDER BY tuple()
        SETTINGS
            disk = disk(
                readonly = true,
                type = object_storage,
                metadata_type = local,
                object_storage_type = azure_blob_storage,
                name = '${DISK_NAME}',
                path='/var/lib/clickhouse/disks/${CONTAINER}/tables',
                container_name = '${CONTAINER}',
                endpoint = 'http://localhost:10000/devstoreaccount1/${CONTAINER}/tables',
                account_name = 'devstoreaccount1',
                account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
    INSERT INTO test_azure_readonly SELECT *, [] from numbers(1); -- { serverError TABLE_IS_READ_ONLY }
    OPTIMIZE TABLE test_azure_readonly FINAL; -- { serverError TABLE_IS_READ_ONLY }
    SELECT 'Disk is read only', is_read_only FROM system.disks WHERE name = '${DISK_NAME}';
    DROP TABLE test_azure_readonly SYNC;
"
