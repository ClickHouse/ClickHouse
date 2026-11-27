#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_BINARY_CH=${CLICKHOUSE_BINARY/clickhouse/ch}

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
