#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT"

echo "SELECT (x.1)[1], (x[1].1)[1].1, (NOT x)[1], -x[1], (-x)[1], (NOT x).1, -x.1, (-x).1" | $format
