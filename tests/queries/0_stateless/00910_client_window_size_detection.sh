#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e
set -o pipefail

${CLICKHOUSE_CLIENT} --help </dev/null | wc -L
script -e -q -c "${CLICKHOUSE_CLIENT} --help" /dev/null </dev/null >/dev/null
