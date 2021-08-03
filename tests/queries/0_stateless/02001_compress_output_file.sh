CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World! From client.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client.gz'"
gunzip ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client.gz
cat ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_client

${CLICKHOUSE_LOCAL} --query "SELECT * FROM (SELECT 'Hello, World! From local.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_compression_of_output_file_from_local.gz'"
gunzip ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_local.gz
cat ${CLICKHOUSE_TMP}/test_compression_of_output_file_from_local