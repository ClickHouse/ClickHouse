#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Azure data lake table engines are not in the fast test build.
# no-msan: delta-kernel-rs is disabled under MSan, so DeltaLakeAzure is unavailable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# IcebergAzure, DeltaLakeAzure and PaimonAzure take the same credential bearing arguments as
# AzureBlobStorage: an `AccountKey=` inside a connection string, or a positional `account_key`. None
# of the three was listed in the table engine dispatch of the secret argument finder, so the storage
# account key stayed verbatim in SHOW CREATE, in system.tables.engine_full and in the logged query
# text, for every user able to read them.
#
# DeltaLakeAzure defers reading the table, so its CREATE succeeds against the unreachable endpoint
# below and SHOW CREATE can be checked directly. IcebergAzure and PaimonAzure read their metadata
# while the table is created, so theirs fail against it; the masking happens when the query is
# formatted for the log, before the failure, so for those two the logged text is what is checked.

KEY="SEKRITACCOUNTKEYSEKRITACCOUNTKEY"
SAS_SIGNATURE="SEKRITSASSIGNATURE"
SAS_TOKEN_SIGNATURE="SEKRITSASTOKENSIGNATURE"
ENDPOINT="http://127.0.0.1:1/devstoreaccount1"
CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=${KEY};BlobEndpoint=${ENDPOINT};"
SAS_URL="${ENDPOINT}/cont?sv=2025-01-05&sp=rl&sr=c&sig=${SAS_SIGNATURE}"
SAS_TOKEN="sv=2025-01-05&sp=rl&sr=c&sig=${SAS_TOKEN_SIGNATURE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_delta_azure_conn"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_delta_azure_key"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_delta_azure_sas"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_delta_azure_conn (x UInt8)
    ENGINE = DeltaLakeAzure('${CONNECTION_STRING}', 'cont', 'p')" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_delta_azure_key (x UInt8)
    ENGINE = DeltaLakeAzure('${ENDPOINT}', 'cont', 'p', 'devstoreaccount1', '${KEY}')" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_delta_azure_sas (x UInt8)
    ENGINE = DeltaLakeAzure('${SAS_URL}', '', 'p')" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_delta_azure_conn"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_delta_azure_key"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_delta_azure_sas"
${CLICKHOUSE_CLIENT} --query "
    SELECT engine_full FROM system.tables
    WHERE database = currentDatabase() AND name LIKE 't_delta_azure%'
    ORDER BY name"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_delta_azure_conn"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_delta_azure_key"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_delta_azure_sas"

# The endpoint is unreachable on purpose; the failure is not what is under test, only the logged text.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_iceberg_azure (x UInt8)
    ENGINE = IcebergAzure('${CONNECTION_STRING}', 'cont', 'p')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine=1 --query "
    CREATE TABLE t_paimon_azure (x UInt8)
    ENGINE = PaimonAzure('${ENDPOINT}', 'cont', 'p', 'devstoreaccount1', '${KEY}')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_iceberg_azure_named (x UInt8)
    ENGINE = IcebergAzure(nc_04908_missing, storage_account_url = '${SAS_URL}')" >/dev/null 2>&1

# The two-argument signature reads its second argument as a shared access signature. The engine
# rewrites its arguments when the table is created, so the logged query text is what is checked.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_iceberg_azure_sas_token (x UInt8)
    ENGINE = IcebergAzure('${ENDPOINT}/cont/p', '${SAS_TOKEN}')" >/dev/null 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# Every statement this test issued, the failed CREATEs included, must be logged without the key.
# count() > 0 keeps an empty row set from passing vacuously.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0, countIf(query LIKE '%${KEY}%' OR query LIKE '%${SAS_SIGNATURE}%' OR query LIKE '%${SAS_TOKEN_SIGNATURE}%')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type != 'QueryStart'
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE"
