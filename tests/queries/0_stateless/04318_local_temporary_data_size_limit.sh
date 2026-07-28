#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local no longer hard-codes the temporary storage limit to 1 GiB:
# it is configurable via the `max_temporary_data_on_disk_size` server setting.

CONFIG_SMALL="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_small.xml"
CONFIG_ZERO="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_zero.xml"

echo "<clickhouse><max_temporary_data_on_disk_size>3000000</max_temporary_data_on_disk_size></clickhouse>" > "${CONFIG_SMALL}"
echo "<clickhouse><max_temporary_data_on_disk_size>0</max_temporary_data_on_disk_size></clickhouse>" > "${CONFIG_ZERO}"

QUERY="SELECT number FROM numbers(10000000) ORDER BY number * 0xABCDEF % 100000000 FORMAT Null"
SETTINGS="--max_bytes_before_external_sort=1000000 --max_bytes_ratio_before_external_sort=0"

# A small limit is honored: the external sort spills more than the limit and fails.
${CLICKHOUSE_LOCAL} -C "${CONFIG_SMALL}" ${SETTINGS} -q "${QUERY}" 2>&1 \
    | grep -o -m1 "Limit for temporary files size exceeded"

# Lifting the limit (0 means unlimited) lets the same query complete.
${CLICKHOUSE_LOCAL} -C "${CONFIG_ZERO}" ${SETTINGS} -q "${QUERY}"
echo $?

rm -f "${CONFIG_SMALL}" "${CONFIG_ZERO}"
