#!/usr/bin/env bash
# shellcheck disable=SC2206

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --unknown-option 2>&1 echo

