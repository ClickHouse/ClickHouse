#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Azurite

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 lowercase alphanumeric characters, so hash the unique name instead of embedding it.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

# Every block is exactly 100 numbers, and a new blob is started as soon as 1000 bytes are written,
# so the resulting blobs are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, azure_split_on_write_by_size_bytes = 1000"

echo '--- Split into numbered blobs'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'split/data.tsv', 'TSV', 'auto', 'x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT _file, count(), min(x), max(x) FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'split/data*.tsv', 'TSV', 'auto', 'x UInt64') GROUP BY _file ORDER BY _file;
    SELECT count(), sum(x) FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'split/data*.tsv', 'TSV', 'auto', 'x UInt64');
"

echo '--- The numbering continues from the number in the name of the first blob'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'offset/data.5.tsv', 'TSV', 'auto', 'x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT _file, count() FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'offset/data*.tsv', 'TSV', 'auto', 'x UInt64') GROUP BY _file ORDER BY _file;
"

echo '--- All the split blobs are visible for the AzureBlobStorage engine table, and a truncating insert overwrites them and removes the leftovers'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05100 (x UInt64) ENGINE = AzureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'engine/data.tsv', 'TSV');
    INSERT INTO test_05100 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), sum(x), uniqExact(_file) FROM test_05100;
    INSERT INTO test_05100 SELECT number FROM numbers(500) SETTINGS ${SETTINGS}, azure_truncate_on_insert = 1;
    SELECT _file, count(), min(x), max(x) FROM test_05100 GROUP BY _file ORDER BY _file;
    SELECT _file, count() FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'engine/data*.tsv', 'TSV', 'auto', 'x UInt64') GROUP BY _file ORDER BY _file;
    DROP TABLE test_05100;
"

echo '--- An exception if the name is already taken'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'exists/data.1.tsv', 'TSV', 'auto', 'x UInt64') SELECT 42;
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'exists/data.tsv', 'TSV', 'auto', 'x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '--- The taken names are skipped with azure_create_new_file_on_insert'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'skip/data.1.tsv', 'TSV', 'auto', 'x UInt64') SELECT 42;
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'skip/data.tsv', 'TSV', 'auto', 'x UInt64')
        SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}, azure_create_new_file_on_insert = 1;
    SELECT _file, count() FROM azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'skip/data*.tsv', 'TSV', 'auto', 'x UInt64') GROUP BY _file ORDER BY _file;
"
