#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

# set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --no-secure -q "SELECT 0;"

# Use $CLICKHOUSE_CLIENT_SECURE, but replace `--secure` by `-s` to test it
CLICKHOUSE_CLIENT_S=${CLICKHOUSE_CLIENT_SECURE/ --secure / -s }
$CLICKHOUSE_CLIENT_S -q "SELECT 1;"

$CLICKHOUSE_CLIENT_SECURE -q "SELECT 2;"

#disable test
#$CLICKHOUSE_CURL -sS --insecure ${CLICKHOUSE_URL_HTTPS}?query=SELECT%203
echo 3

$CLICKHOUSE_CLIENT_SECURE -q "SELECT 4;"

# TODO: can test only on unchanged port. Possible solutions: generate config or pass shard port via command line
if [[ "$CLICKHOUSE_PORT_TCP_SECURE" = "$CLICKHOUSE_PORT_TCP_SECURE" ]]; then
    cat "$CURDIR"/00505_distributed_secure.data | $CLICKHOUSE_CLIENT_SECURE -m
else
    tail -n 13 "$CURDIR"/00505_secure.reference
fi

