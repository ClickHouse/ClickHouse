#!/usr/bin/env bash

# Tags: no-fasttest
# no-fasttest: requires azureBlobStorage function

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


for id in {1..6}; do
  $CLICKHOUSE_CLIENT -q "
  INSERT INTO FUNCTION
  azureBlobStorage(
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;',
    '03582azuredoublefreecont3',
    'blob$id.native',
    'Native',
    'auto',
    'a UInt64'
  )
  SELECT *
  FROM numbers_mt(1e3)
  SETTINGS azure_truncate_on_insert = 1
  "
done

# Previously, because we can call `AzureIteratorAsync::getBatchAndCheckNext()` from a thread pool after destruction of a `AzureIteratorAsync` object,
# the 'libc++abi: Pure virtual function called!' error was happening under the ASan.
for _ in {1..15}; do
  $CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM azureBlobStorage(
      'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;',
      '03582azuredoublefreecont3',
      'blob*'
    )
    LIMIT 1
    FORMAT Null
    SETTINGS azure_list_object_keys_size = 1, max_block_size = 1, max_threads = 1
  "
done

exit 0
