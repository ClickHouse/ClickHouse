#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query=select%201&log_queries=1"
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&&query=select%201&log_queries=1"
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query=select%201&&&log_queries=1"
