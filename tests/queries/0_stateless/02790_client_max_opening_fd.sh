#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Ensure that clickhouse-client does not open a large number of files.
ulimit -n 1024
${CLICKHOUSE_CLIENT} --query "SELECT 1"
