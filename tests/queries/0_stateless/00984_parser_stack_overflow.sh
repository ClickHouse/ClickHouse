#!/usr/bin/env bash

# Such a huge timeout mostly for debug build.
CLICKHOUSE_CURL_TIMEOUT=60

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Too deep recursion
perl -e 'print "(" x 10000' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -oF 'exceeded'
perl -e 'print "SELECT " . ("[" x 10000)' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -oF 'exceeded'
perl -e 'print "SELECT " . ("([" x 5000)' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -oF 'exceeded'
perl -e 'print "SELECT 1" . ("+1" x 10000)' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | grep -oF 'exceeded'

# But this is Ok
perl -e 'print "SELECT 1" . (",1" x 10000)' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @- | wc -c
perl -e 'print "SELECT 1" . (" OR 1" x 10000)' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @-
