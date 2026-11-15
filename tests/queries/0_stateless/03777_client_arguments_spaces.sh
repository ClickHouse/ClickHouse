#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT getSetting('max_threads')" --max_threads=1

$CLICKHOUSE_CLIENT -q "SELECT getSetting('max_threads')" --max_threads = 2
