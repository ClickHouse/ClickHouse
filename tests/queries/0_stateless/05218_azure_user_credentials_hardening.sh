#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs USE_AZURE_BLOB_STORAGE and Azurite.
#
# Azure access from user SQL may not authenticate with the server's own identity. The restriction is applied
# when the client is built, so the refused cases need no reachable endpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

NC_URL_ONLY="azure_url_only_${DB}"
NC_ACCOUNT_KEY="azure_account_key_${DB}"

TABLE="azure_hardening_${DB}"
DISK_TABLE="azure_hardening_disk_${DB}"
OPT_IN_TABLE="azure_hardening_opt_in_${DB}"

# The Azure SDK attaches a bearer token only to an `https` endpoint.
URL="https://localhost:11111/${DB}"

# Azurite, for the permitted cases that can actually read and write.
AZURITE_URL="http://localhost:10000/devstoreaccount1"
AZURITE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=${AZURITE_KEY};BlobEndpoint=${AZURITE_URL};"
CONTAINER="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"

cleanup() {
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS ${TABLE};
        DROP TABLE IF EXISTS ${DISK_TABLE} SYNC;
        DROP TABLE IF EXISTS ${OPT_IN_TABLE} SYNC;
        DROP NAMED COLLECTION IF EXISTS ${NC_URL_ONLY};
        DROP NAMED COLLECTION IF EXISTS ${NC_ACCOUNT_KEY};
    " 2>/dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_URL_ONLY} AS
        storage_account_url = '${URL}', container = 'cont', blob_path = 'f.csv';
    CREATE NAMED COLLECTION ${NC_ACCOUNT_KEY} AS
        storage_account_url = '${AZURITE_URL}', container = '${CONTAINER}', blob_path = 'f.csv',
        account_name = 'devstoreaccount1', account_key = '${AZURITE_KEY}';
    INSERT INTO FUNCTION azureBlobStorage('${CONN_STRING}', '${CONTAINER}', 'f.csv', 'CSV', 'auto', 'x UInt8')
        SELECT 7 SETTINGS azure_truncate_on_insert = 1;
" > /dev/null

# Classifies without depending on which error a permitted but unreachable endpoint produces.
run_and_classify() {
    local description="$1"
    local query="$2"
    shift 2
    if $CLICKHOUSE_CLIENT --azure_sdk_max_retries=0 "$@" -q "${query}" 2>&1 | grep -qF 'ACCESS_DENIED'; then
        echo "restricted: ${description}"
    else
        echo "allowed: ${description}"
    fi
}

# Refused: no explicit credentials, so the server's own identity would be used.
# `DESCRIBE` omits the structure: with one given, the table function never builds a client.
$CLICKHOUSE_CLIENT -m -q "
    SELECT * FROM azureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV', 'auto', 'x UInt8'); -- { serverError ACCESS_DENIED }
    DESCRIBE TABLE azureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV'); -- { serverError ACCESS_DENIED }
    SELECT * FROM azureBlobStorage(${NC_URL_ONLY}, format = 'CSV', structure = 'x UInt8'); -- { serverError ACCESS_DENIED }
"

# `extra_credentials` still presents the server's own federated token to Entra ID.
$CLICKHOUSE_CLIENT -q "
    SELECT * FROM azureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV', 'auto', 'x UInt8',
        extra_credentials(client_id = 'c0ffee00-0000-0000-0000-000000000000',
                          tenant_id = 'deadbeef-0000-0000-0000-000000000000')); -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = AzureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV'); -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO ${TABLE} VALUES (1);
    BACKUP TABLE ${TABLE} TO AzureBlobStorage('${URL}', 'cont', 'b_${DB}'); -- { serverError ACCESS_DENIED }
" > /dev/null

# A dynamic disk from user SQL is checked as well, unlike a disk from the server configuration.
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = '${URL}', container_name = 'cont'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = '${URL}', container_name = 'cont',
            use_workload_identity = 1); -- { serverError ACCESS_DENIED }
"

# A disk created by a session with the opt-in is reused, not built again, by a later identical definition.
# `readonly`, `container_already_exists` and `skip_access_check` keep it from contacting the endpoint.
OPT_IN_DISK="disk(type = azure_blob_storage, storage_account_url = '${URL}', container_name = 'cont',
    readonly = 1, container_already_exists = 1, skip_access_check = 1)"
$CLICKHOUSE_CLIENT --azure_allow_server_credentials_in_user_queries=1 -q "
    CREATE TABLE ${OPT_IN_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${OPT_IN_DISK}"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${OPT_IN_DISK}; -- { serverError ACCESS_DENIED }
"

# Permitted: explicit credentials read and write for real.
echo "connection string: $($CLICKHOUSE_CLIENT -q "
    SELECT * FROM azureBlobStorage('${CONN_STRING}', '${CONTAINER}', 'f.csv', 'CSV', 'auto', 'x UInt8')")"

echo "account name and key in a named collection: $($CLICKHOUSE_CLIENT -q "
    SELECT * FROM azureBlobStorage(${NC_ACCOUNT_KEY}, format = 'CSV', structure = 'x UInt8')")"

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = '${AZURITE_URL}',
            container_name = '${CONTAINER}', account_name = 'devstoreaccount1', account_key = '${AZURITE_KEY}');
    INSERT INTO ${DISK_TABLE} VALUES (7);
"
echo "dynamic disk with account name and key: $($CLICKHOUSE_CLIENT -q "SELECT * FROM ${DISK_TABLE}")"

# Permitted without a reachable endpoint, so only checked for not being refused.

run_and_classify "SAS token in the endpoint" \
    "SELECT * FROM azureBlobStorage('${URL}?sv=2021-08-06&sig=abc', 'cont', 'f.csv', 'CSV', 'auto', 'x UInt8')"

run_and_classify "server identity with the opt-in enabled" \
    "SELECT * FROM azureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV', 'auto', 'x UInt8')" \
    --azure_allow_server_credentials_in_user_queries=1

# In clickhouse-local the user is the operator.
if ${CLICKHOUSE_LOCAL} --azure_sdk_max_retries=0 -q "
    SELECT * FROM azureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV', 'auto', 'x UInt8')" 2>&1 | grep -qF 'ACCESS_DENIED'; then
    echo "restricted: clickhouse-local"
else
    echo "allowed: clickhouse-local"
fi
