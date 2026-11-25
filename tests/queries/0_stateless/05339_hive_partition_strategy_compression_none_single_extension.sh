#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires Azurite

# A `hive` table with `compression_method = 'none'` over a format with a single file extension
# must read the bare files and skip the compressed ones that `auto` reads.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION azureBlobStorage('$AZURE_CONN', '$AZURE_CONT', 'lake/key=1/data.csv', 'CSV', 'auto', 'id UInt64') SELECT 1 SETTINGS azure_truncate_on_insert = 1;
INSERT INTO FUNCTION azureBlobStorage('$AZURE_CONN', '$AZURE_CONT', 'lake/key=2/data.csv.gz', 'CSV', 'auto', 'id UInt64') SELECT 2 SETTINGS azure_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_auto;
DROP TABLE IF EXISTS t_none;

CREATE TABLE t_auto (id UInt64, key UInt64)
ENGINE = AzureBlobStorage('$AZURE_CONN', '$AZURE_CONT', 'lake', 'CSV', 'auto', 'hive')
PARTITION BY key;

CREATE TABLE t_none (id UInt64, key UInt64)
ENGINE = AzureBlobStorage('$AZURE_CONN', '$AZURE_CONT', 'lake', 'CSV', 'none', 'hive')
PARTITION BY key;

SELECT 'auto:';
SELECT id, key FROM t_auto ORDER BY id;

SELECT 'none:';
SELECT id, key FROM t_none ORDER BY id;

DROP TABLE t_auto;
DROP TABLE t_none;
"
