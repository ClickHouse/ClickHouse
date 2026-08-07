#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Access by name"
echo "('2e5d8c78-4e4e-488f-84c5-31222482eaa6',2)" | ${CLICKHOUSE_CLIENT} \
    --query "SELECT x.a, x.b FROM _data" \
    --external \
    --file=- \
    --name=_data \
    --structure='x Tuple(a UUID, b Int32)'

echo "Access by index"
echo "('2e5d8c78-4e4e-488f-84c5-31222482eaa6',2)" | ${CLICKHOUSE_CLIENT} \
    --query "SELECT x.1, x.2 FROM _data" \
    --external \
    --file=- \
    --name=_data \
    --structure='x Tuple(a UUID, b Int32)'

echo "Nullable subcolumn"
echo "1" | ${CLICKHOUSE_CLIENT} \
    --query "SELECT x.null FROM _data" \
    --external \
    --file=- \
    --name=_data \
    --structure='x Nullable(UInt32)'
