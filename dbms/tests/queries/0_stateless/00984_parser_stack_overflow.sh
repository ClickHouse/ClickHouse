#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

perl -e 'print "(" x 10000 ' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -oF 'exceeded'
