#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan
# ^ requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Inaccessible IMDS should not introduce large delays, so this query should reply quickly at least sometimes:
while true
do
    # This host (likely) drops packets sent to it (does not reply), so it is good for testing timeouts.
    # At the same time, we expect that the clickhouse host does not drop packets and quickly replies with 4xx, which is a non-retriable error for S3.
    AWS_EC2_METADATA_SERVICE_ENDPOINT='https://10.255.255.255/' ${CLICKHOUSE_LOCAL} --time --query "SELECT * FROM s3('${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/nonexistent')" |& grep -v -F 404 |
        ${CLICKHOUSE_LOCAL} -S 'c1 Float64' --input-format TSV "SELECT c1::Float64 < 1 FROM table" | grep 1 && break
done
