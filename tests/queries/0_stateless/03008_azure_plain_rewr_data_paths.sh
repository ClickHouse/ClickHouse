#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-distributed-cache, no-replicated-database
# Tag no-fasttest: requires Azure
# Tag no-shared-merge-tree: does not support replication
# Tag no-distributed-cache: Not supported auth type
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Base container name
base_container="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"

declare -A disk_configs

disk_configs[url]="disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '%s',
    container_name = '%s',
    storage_account_url = 'http://localhost:10000/devstoreaccount1',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
)"

disk_configs[endpoint1]="disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '%s',
    endpoint = 'http://localhost:10000/devstoreaccount1/%s',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
)"

disk_configs[endpoint2]="disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '%s',
    endpoint = 'http://localhost:10000/devstoreaccount1/%s/system-tables',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
)"

disk_configs[endpoint3]="disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '%s',
    endpoint = 'http://localhost:10000/devstoreaccount1/%s/system-tables/',
    endpoint_subpath = 'subpath',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
)"

disk_configs[conn]="disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '%s',
    container_name = '%s',
    connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;'
)"

declare -A expected_prefix
expected_prefix[url]="%s/"
expected_prefix[endpoint1]="%s/"
expected_prefix[endpoint2]="%s/system-tables/"
expected_prefix[endpoint3]="%s/system-tables/subpath/"
expected_prefix[conn]="%s/"


for config in $(printf "%s\n" "${!disk_configs[@]}" | sort); do
    container="${base_container}-${config//_/-}"
    table="03008_azure_data_paths_${config}"

    disk_setting=$(printf "${disk_configs[$config]}" "$container" "$container")
    expected_prefix=$(printf "${expected_prefix[$config]}" "$container")

    echo "=== Running test for configuration: $config ==="

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"

    ${CLICKHOUSE_CLIENT} -nm --query "
    CREATE TABLE ${table} (a Int32, b Int32)
    ENGINE = MergeTree()
    ORDER BY a
    SETTINGS disk = $disk_setting;
    "

    echo "-- Checking data paths for ${table}"
    ${CLICKHOUSE_CLIENT} --query "
    SELECT count(path) >= 1 FROM system.tables
    ARRAY JOIN data_paths AS path
    WHERE database = currentDatabase()
        AND name = '${table}'
        AND startsWith(path, '${expected_prefix}');
    "

    ${CLICKHOUSE_CLIENT} --query "
    SELECT path FROM system.tables
    ARRAY JOIN data_paths AS path
    WHERE database = currentDatabase()
        AND name = '${table}'
        AND NOT startsWith(path, '${expected_prefix}');
    "

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"

    echo
done
