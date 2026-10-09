#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# `no-fasttest`: the MaxCompute engine is not built in the fast-test image.
# `no-replicated-database`: replicated DDL does not execute as the initiating user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="maxcompute_source_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="maxcompute_${CLICKHOUSE_TEST_UNIQUE_NAME}"
RAW_TABLE="maxcompute_raw_${CLICKHOUSE_TEST_UNIQUE_NAME}"
INVALID_TABLE="maxcompute_invalid_${CLICKHOUSE_TEST_UNIQUE_NAME}"
COLLECTION="maxcompute_collection_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${RAW_TABLE} SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${INVALID_TABLE} SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${COLLECTION}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"

if ${CLICKHOUSE_CLIENT} --allow_experimental_maxcompute_storage_engine=1 --user "${USER}" -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} (value UInt64) ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret')" \
    2>&1 | grep -qiF "necessary to have the grant TABLE ENGINE ON MaxCompute"; then
    echo "MaxCompute denied without source grant"
else
    echo "MaxCompute was not denied as expected"
fi

if ${CLICKHOUSE_CLIENT} --allow_experimental_maxcompute_storage_engine=1 --user "${USER}" -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.${RAW_TABLE} (value UInt64) ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret')" \
    2>&1 | grep -qiF "necessary to have the grant TABLE ENGINE ON MaxComputeRaw"; then
    echo "MaxComputeRaw denied without source grant"
else
    echo "MaxComputeRaw was not denied as expected"
fi

${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON MAXCOMPUTE TO ${USER}"
${CLICKHOUSE_CLIENT} --allow_experimental_maxcompute_storage_engine=1 --user "${USER}" -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} (value UInt64) ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret')"
${CLICKHOUSE_CLIENT} --allow_experimental_maxcompute_storage_engine=1 --user "${USER}" -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.${RAW_TABLE} (value UInt64) ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret')"

${CLICKHOUSE_CLIENT} -q \
    "SELECT engine FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('${TABLE}', '${RAW_TABLE}') ORDER BY engine"

${CLICKHOUSE_CLIENT} -q \
    "CREATE NAMED COLLECTION ${COLLECTION} AS endpoint = 'https://tunnel.example', project = 'project', \`table\` = 'table', access_key_id = 'access_id', access_key_secret = 'secret'"
if ${CLICKHOUSE_CLIENT} --allow_experimental_maxcompute_storage_engine=1 -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.${INVALID_TABLE} (value Array(UInt64)) ENGINE = MaxCompute(${COLLECTION})" \
    2>&1 | grep -qiF "Unsupported column type: Array(UInt64)"; then
    echo "named collection schema rejected"
else
    echo "named collection schema was not rejected as expected"
fi
