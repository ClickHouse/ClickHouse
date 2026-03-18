#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo client
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -q "insert into function null('foo Int') values (1)" |& grep -o 'InsertedRows: .*'

echo local
$CLICKHOUSE_LOCAL  --print-profile-events --profile-events-delay-ms=-1 -q "insert into function null('foo Int') values (1)" |& grep -o 'InsertedRows: .*'

exit 0
