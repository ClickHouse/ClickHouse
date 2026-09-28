#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eu

# server does not send the message body at HTTP HEAD request
# client closes connection just to save time

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/does_not_exist"

echo HEAD does not have a message
${CLICKHOUSE_CURL} -H 'Connection: close' -X HEAD "${URL}" -D - | grep -v 'Date:'

echo GET has a message
${CLICKHOUSE_CURL} -H 'Connection: close' -X GET "${URL}" -D - | grep 'There is no handle /does_not_exist'
