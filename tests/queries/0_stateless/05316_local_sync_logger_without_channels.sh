#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Synchronous logger with no log destination and one logger level raised explicitly.
$CLICKHOUSE_LOCAL --query "SELECT 1" -- --logger.async=0 --logger.levels.Application=trace
