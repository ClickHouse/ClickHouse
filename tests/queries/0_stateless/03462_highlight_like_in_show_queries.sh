#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY="SHOW TABLES ILIKE '%te_st%'"

TERM=xterm-256color ${CLICKHOUSE_FORMAT} --hilite <<< "$QUERY"

TERM=xterm-256color ${CLICKHOUSE_FORMAT} --highlight <<< "$QUERY"

