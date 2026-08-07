#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --seed Hello --obfuscate <<< "SELECT regionToName(213), regionToCountry(213)"
