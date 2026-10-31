#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-fasttest: an Azure disk
# no-shared-merge-tree: custom disk

# A filtered `READ ON AZURE` grant is matched against the location of an Azure disk described with
# `storage_account_url` and `container_name` (or `container`): `<storage_account_url>/<container>/`.
# A `connection_string` wins over the `storage_account_url` when the disk is created, so the URL
# is not trusted then and the unfiltered grant is required.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_mtpa_${CLICKHOUSE_DATABASE}"
ACCOUNT_URL="http://localhost:10000/devstoreaccount1"
CREDENTIALS="account_name = 'devstoreaccount1', account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='"

function mtp()
{
    echo "mergeTreeParts(structure('id Int64'), parts(), disk(type = azure_blob_storage, $1, ${CREDENTIALS}), table_settings(index_granularity_bytes = 10485760))"
}

# Only whether the access check passes matters here, not what happens with the disk after it.
function check()
{
    ${CLICKHOUSE_CLIENT} --user "${USER}" --query "$1" 2>&1 | grep -q "Missing permissions: READ ON AZURE\|grant READ ON AZURE" && echo "ACCESS_DENIED" || echo "passed the access check"
}

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER};
    GRANT READ ON AZURE('${ACCOUNT_URL}/mtpa-allowed/.*') TO ${USER};"

echo "-- READ ON AZURE filtered to one container"
check "SELECT count() FROM $(mtp "storage_account_url = '${ACCOUNT_URL}', container_name = 'mtpa-allowed'")"
check "DESCRIBE $(mtp "storage_account_url = '${ACCOUNT_URL}', container_name = 'mtpa-allowed'")"
check "SELECT count() FROM $(mtp "storage_account_url = '${ACCOUNT_URL}/', container = 'mtpa-allowed'")"
check "SELECT count() FROM $(mtp "storage_account_url = '${ACCOUNT_URL}', container_name = 'mtpa-other'")"
check "DESCRIBE $(mtp "storage_account_url = '${ACCOUNT_URL}', container_name = 'mtpa-other'")"
check "SELECT count() FROM $(mtp "storage_account_url = '${ACCOUNT_URL}', connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=a;BlobEndpoint=http://localhost:10000/devstoreaccount1;', container_name = 'mtpa-allowed'")"

echo "-- READ ON AZURE without a filter"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON AZURE TO ${USER}"
check "SELECT count() FROM $(mtp "storage_account_url = '${ACCOUNT_URL}', container_name = 'mtpa-other'")"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
