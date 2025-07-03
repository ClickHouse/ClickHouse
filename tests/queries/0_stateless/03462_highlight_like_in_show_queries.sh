#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TERM=xterm-256color ${CLICKHOUSE_FORMAT} --hilite <<< "SHOW TABLES ILIKE '%te_st%'"
