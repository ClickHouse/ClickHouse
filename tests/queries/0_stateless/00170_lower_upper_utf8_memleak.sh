#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: upper/lowerUTF8 use ICU

# Test for issue #69336

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT lowerUTF8('ESPAÑA')"
