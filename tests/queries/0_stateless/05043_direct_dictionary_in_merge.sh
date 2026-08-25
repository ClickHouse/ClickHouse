#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

python3 "$CURDIR"/05043_direct_dictionary_in_merge.python

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS dict_direct"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS dict_hashed"
