#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --file /dev/null --structure "key String" --input-format TSVWithNamesAndTypes --queries-file <(echo 'select 1') --queries-file <(echo 'select 2') --format Null
