#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TERMINATE_ON_ANY_EXCEPTION=1 ${CLICKHOUSE_LOCAL} --query "SELECT * FROM table" --input-format CSV <<<"Hello, world"
