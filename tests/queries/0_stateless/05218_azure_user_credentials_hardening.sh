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
QUEUE="azure_hardening_queue_${DB}"
FROM_ZK_TABLE="azure_hardening_from_zk_${DB}"
NAMED_DISK_TABLE="azure_hardening_named_disk_${DB}"
IDENTITY_QUEUE="azure_hardening_identity_queue_${DB}"
NAMED_DISK="azure_hardening_trusted_${DB}"
ZK_PATH="/clickhouse/test_azure_hardening_${DB}"

# The Azure SDK attaches a bearer token only to an `https` endpoint.
URL="https://localhost:11111/${DB}"
# A disk endpoint allows only lowercase letters and digits in the path.
DISK_URL="https://localhost:11111/$(echo "${DB}" | tr -d _)"

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
        DROP TABLE IF EXISTS ${QUEUE} SYNC;
        DROP TABLE IF EXISTS ${FROM_ZK_TABLE} SYNC;
        DROP TABLE IF EXISTS ${NAMED_DISK_TABLE} SYNC;
        DROP TABLE IF EXISTS ${IDENTITY_QUEUE} SYNC;
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
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = '${DISK_URL}', container_name = 'cont'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = '${DISK_URL}', container_name = 'cont',
            use_workload_identity = 1); -- { serverError ACCESS_DENIED }
"

# A disk created by a session with the opt-in is reused, not built again, by a later identical definition.
# `readonly`, `container_already_exists` and `skip_access_check` keep it from contacting the endpoint.
OPT_IN_DISK="disk(type = azure_blob_storage, storage_account_url = '${DISK_URL}', container_name = 'cont',
    readonly = 1, container_already_exists = 1, skip_access_check = 1)"
$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries=1 -q "
    CREATE TABLE ${OPT_IN_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${OPT_IN_DISK}"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${OPT_IN_DISK}; -- { serverError ACCESS_DENIED }
    -- The marker that keeps an opted-in disk's credentials across a restart is ignored in a query.
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = '${DISK_URL}', container_name = 'cont',
            _azure_server_credentials_allowed = 1); -- { serverError ACCESS_DENIED }
"

# As for `S3Queue`, a move destination must carry its own credentials, whoever creates or alters the queue.
$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries=1 -m -q "
    CREATE TABLE ${QUEUE} (x UInt8) ENGINE = AzureQueue('${CONN_STRING}', '${CONTAINER}', 'queue/*', 'CSV')
        SETTINGS mode = 'unordered', after_processing = 'move',
            after_processing_move_connection_string = '${URL}', after_processing_move_container = 'cont'; -- { serverError ACCESS_DENIED }
    CREATE TABLE ${QUEUE} (x UInt8) ENGINE = AzureQueue('${CONN_STRING}', '${CONTAINER}', 'queue/*', 'CSV')
        SETTINGS mode = 'unordered', after_processing = 'move',
            after_processing_move_connection_string = '${CONN_STRING}', after_processing_move_container = '${CONTAINER}';
"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE ${QUEUE} MODIFY SETTING after_processing_move_connection_string = '${URL}'; -- { serverError ACCESS_DENIED }
"

# A queue created with the opt-in keeps it for background processing, but a restricted direct reader is refused.
$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries=1 -q "
    CREATE TABLE ${IDENTITY_QUEUE} (x UInt8) ENGINE = AzureQueue('${URL}', 'cont', 'queue/*', 'CSV') SETTINGS mode = 'unordered'"
$CLICKHOUSE_CLIENT -q "
    SELECT * FROM ${IDENTITY_QUEUE} SETTINGS stream_like_engine_allow_direct_select = 1; -- { serverError ACCESS_DENIED }
"

# A wrapper may name a disk an opted-in session created; the restricted session is refused, not given that disk.
$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries=1 -q "
    CREATE TABLE ${NAMED_DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(name = '${NAMED_DISK}', type = azure_blob_storage, storage_account_url = '${DISK_URL}',
            container_name = 'cont', readonly = 1, container_already_exists = 1, skip_access_check = 1)"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${DISK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = encrypted, disk = '${NAMED_DISK}', key = '0123456789abcdef', path = 'restricted/'); -- { serverError ACCESS_DENIED }
"

# The endpoint of an opted-in disk may come from `from_zk`, which is substituted only after the opt-in is recorded.
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('${ZK_PATH}', 'url', '${DISK_URL}')"
$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries=1 --dynamic_disk_allow_from_zk=1 -q "
    CREATE TABLE ${FROM_ZK_TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = 'from_zk ${ZK_PATH}/url', container_name = 'cont',
            readonly = 1, container_already_exists = 1, skip_access_check = 1)"
echo "opt-in recorded for a from_zk disk: $($CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE ${FROM_ZK_TABLE}" | grep -c '_azure_server_credentials_allowed')"

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
    --s3_allow_server_credentials_in_user_queries=1

# In clickhouse-local the user is the operator.
if ${CLICKHOUSE_LOCAL} --azure_sdk_max_retries=0 -q "
    SELECT * FROM azureBlobStorage('${URL}', 'cont', 'f.csv', 'CSV', 'auto', 'x UInt8')" 2>&1 | grep -qF 'ACCESS_DENIED'; then
    echo "restricted: clickhouse-local"
else
    echo "allowed: clickhouse-local"
fi
