#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_ACCESS="test_04498_drop_detached_access"
USER_ACCESS="u_04498_drop_detached_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function query()
{
    ${CLICKHOUSE_CLIENT} --query "$1"
}

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS ${TABLE_ACCESS} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_ACCESS} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER_ACCESS}" 2>/dev/null ||:
}

trap cleanup EXIT
cleanup

query "CREATE TABLE ${TABLE_ACCESS} (number UInt64) ENGINE=MergeTree ORDER BY number"
query "DETACH TABLE ${TABLE_ACCESS} PERMANENTLY"
if [[ "$(query "SELECT count() > 0 FROM system.clusters WHERE cluster='test_shard_localhost'")" == "1" ]]
then
    gate_error="$(${CLICKHOUSE_CLIENT} \
        --distributed_ddl_output_mode=none \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=0; DROP DETACHED TABLE ${TABLE_ACCESS} ON CLUSTER test_shard_localhost SYNC" 2>&1 || true)"
    if [[ "${gate_error}" != *"allow_experimental_drop_detached_table"* ]]
    then
        echo "DROP DETACHED TABLE ON CLUSTER ignored disabled feature gate"
        exit 1
    fi
    query "SELECT count() FROM system.detached_tables WHERE database=currentDatabase() AND table='${TABLE_ACCESS}'"

    query "CREATE USER ${USER_ACCESS}"
    query "GRANT CLUSTER ON *.* TO ${USER_ACCESS}"
    query "GRANT DROP TABLE ON ${CLICKHOUSE_DATABASE}.${TABLE_ACCESS} TO ${USER_ACCESS}"

    ${CLICKHOUSE_CLIENT} \
        --user="${USER_ACCESS}" \
        --distributed_ddl_output_mode=none \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE ${TABLE_ACCESS} ON CLUSTER test_shard_localhost SYNC"
else
    gate_error="$(${CLICKHOUSE_CLIENT} \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=0; DROP DETACHED TABLE ${TABLE_ACCESS} SYNC" 2>&1 || true)"
    if [[ "${gate_error}" != *"allow_experimental_drop_detached_table"* ]]
    then
        echo "DROP DETACHED TABLE ignored disabled feature gate"
        exit 1
    fi
    query "SELECT count() FROM system.detached_tables WHERE database=currentDatabase() AND table='${TABLE_ACCESS}'"

    ${CLICKHOUSE_CLIENT} \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE ${TABLE_ACCESS} SYNC"
fi

query "SELECT count() FROM system.detached_tables WHERE database=currentDatabase() AND table='${TABLE_ACCESS}'"

echo "on cluster access: OK"
