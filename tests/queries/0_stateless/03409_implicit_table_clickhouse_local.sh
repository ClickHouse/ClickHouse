#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '{"Hello": {"world": 123}, "goodbye": "test"}' | ${CLICKHOUSE_LOCAL} -q "Hello.world, goodbye" --input-format JSONEachRow
