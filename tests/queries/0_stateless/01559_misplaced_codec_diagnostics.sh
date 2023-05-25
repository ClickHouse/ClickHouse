#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (c CODEC(NONE)) ENGINE = Memory" 2>&1 | grep -oF 'Unknown data type family: CODEC' | uniq
