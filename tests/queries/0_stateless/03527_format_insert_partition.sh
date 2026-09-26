#!/usr/bin/env bash
# The words "PARTITION BY" were not highlighted in previous versions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="INSERT INTO FUNCTION
   s3(
       'https://a_path_to_s3/bucket_name/test.parquet',
       'access_key',
       'secret_key',
       'Parquet'
    ) PARTITION BY rand() % 10 SELECT
    *
FROM TestTable
LIMIT 10;
"

$CLICKHOUSE_FORMAT --query "$QUERY" --hilite --show_secrets

$CLICKHOUSE_FORMAT --query "$QUERY" --highlight --show_secrets

