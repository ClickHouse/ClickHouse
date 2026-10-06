#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires Azure Blob Storage
# An Iceberg table created on Azure with write_full_path_in_iceberg_metadata records an abfss:// location and reads back its own writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL='http://localhost:10000/devstoreaccount1'
KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m -q "
    SET allow_insert_into_iceberg = 1;
    SET write_full_path_in_iceberg_metadata = 1;
    SET iceberg_metadata_compression_method = '';
    SET allow_experimental_iceberg_compaction = 0;
    DROP TABLE IF EXISTS t_iceberg_azure_full_path;
    CREATE TABLE t_iceberg_azure_full_path (x Int32, s String)
        ENGINE = IcebergAzure('${URL}', 'cont', '${DIR}/', 'devstoreaccount1', '${KEY}');
    INSERT INTO t_iceberg_azure_full_path VALUES (1, 'a'), (2, 'b');
    SELECT * FROM t_iceberg_azure_full_path ORDER BY x;
    DROP TABLE t_iceberg_azure_full_path;
"

${CLICKHOUSE_CLIENT} -m -q "
    SELECT _file, replaceOne(JSONExtractString(c, 'location'), '${DIR}', '<dir>')
    FROM azureBlobStorage('${URL}', 'cont', '${DIR}/metadata/*.metadata.json', 'devstoreaccount1', '${KEY}', 'RawBLOB', 'auto', 'c String')
    ORDER BY _file;
    SELECT count(), sum(x) FROM icebergAzure('${URL}', 'cont', '${DIR}/', 'devstoreaccount1', '${KEY}');
"
