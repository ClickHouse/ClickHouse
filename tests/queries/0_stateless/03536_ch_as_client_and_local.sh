#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Run the binary through a path that contains `clickhouse` in a directory component,
# so that deriving the `ch` name must replace only the trailing `clickhouse`
# (a first-match replacement `${CLICKHOUSE_BINARY/clickhouse/ch}` would break the directory name).
CLICKHOUSE_BINARY_DIR="${CLICKHOUSE_TMP}/clickhouse-dir-${CLICKHOUSE_TEST_UNIQUE_NAME}"
CLICKHOUSE_BINARY_REAL=$(command -v "${CLICKHOUSE_BINARY}")
rm -rf "${CLICKHOUSE_BINARY_DIR}"
mkdir -p "${CLICKHOUSE_BINARY_DIR}"
ln -s "${CLICKHOUSE_BINARY_REAL}" "${CLICKHOUSE_BINARY_DIR}/clickhouse"
ln -s "${CLICKHOUSE_BINARY_REAL}" "${CLICKHOUSE_BINARY_DIR}/ch"

CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY_DIR}/clickhouse"
CLICKHOUSE_BINARY_CH=${CLICKHOUSE_BINARY/%clickhouse/ch}

# Invocation with unknown tool name prints help:
${CLICKHOUSE_BINARY} test 2>&1 | grep -F 'Use one of the following commands'

# Invocation with --help works the same:
${CLICKHOUSE_BINARY} --help 2>&1 | grep -F 'Use one of the following commands'
${CLICKHOUSE_BINARY_CH} --help 2>&1 | grep -F 'Use one of the following commands'

# This is recognized as clickhouse-local:
${CLICKHOUSE_BINARY} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()"
${CLICKHOUSE_BINARY_CH} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()"

# This is recognized as clickhouse-client:
${CLICKHOUSE_BINARY} --host ${CLICKHOUSE_HOST} --port ${CLICKHOUSE_PORT_TCP} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()"
${CLICKHOUSE_BINARY_CH} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()" -h${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT_TCP}

rm -rf "${CLICKHOUSE_BINARY_DIR}"
