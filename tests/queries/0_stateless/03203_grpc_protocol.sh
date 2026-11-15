#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [[ -z "$CLICKHOUSE_GRPC_CLIENT" ]]; then
  CLICKHOUSE_GRPC_CLIENT="$CURDIR/../../../utils/grpc-client/clickhouse-grpc-client.py"
fi

# Simple test.
$CLICKHOUSE_GRPC_CLIENT --query "SELECT 'ok'"
