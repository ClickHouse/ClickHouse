#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

obf="$CLICKHOUSE_FORMAT --obfuscate"

echo "CREATE TABLE test (secret1 DateTime('UTC'), secret2 DateTime('Europe/Amsterdam')) ENGINE = ReplicatedVersionedCollapsingMergeTree ORDER BY secret1 SETTINGS index_granularity = 8192;" | $obf
