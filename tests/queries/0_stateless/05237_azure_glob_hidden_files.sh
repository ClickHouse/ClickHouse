#!/usr/bin/env bash
# The hidden-path filtering of 05236_object_storage_glob_hidden_files for the AzureBlobStorage
# engine: the listing layer is shared, this covers the azure_skip_hidden_files wiring and the
# hive partition strategy over Azure.
# Tags: no-fasttest
# Tag no-fasttest: requires azureBlobStorage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

conn="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
container="05237hidden${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION azureBlobStorage('$conn', '$container', 't/dt=2026-01-01/a.parquet', 'Parquet', 'auto', 'id Int64') SELECT toInt64(1) AS id;
INSERT INTO FUNCTION azureBlobStorage('$conn', '$container', 't/_temporary/0/_temporary/attempt_x/dt=2026-01-01/b.parquet', 'Parquet', 'auto', 'id Int64') SELECT toInt64(2) AS id;
INSERT INTO FUNCTION azureBlobStorage('$conn', '$container', 't/.spark-staging-y/dt=2026-01-01/c.parquet', 'Parquet', 'auto', 'id Int64') SELECT toInt64(3) AS id;

CREATE TABLE 05237_hidden (id Int64, dt String)
ENGINE = AzureBlobStorage('$conn', '$container', 't', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY dt;

SELECT 'hive count:', count() FROM 05237_hidden;
SELECT 'hive rows:', id, dt FROM 05237_hidden ORDER BY id;

DROP TABLE 05237_hidden;

SELECT 'plain glob default:', count() FROM azureBlobStorage('$conn', '$container', 't/**.parquet', 'Parquet', 'auto', 'id Int64');
SELECT 'plain glob skip hidden:', count() FROM azureBlobStorage('$conn', '$container', 't/**.parquet', 'Parquet', 'auto', 'id Int64') SETTINGS azure_skip_hidden_files = 1;
"
