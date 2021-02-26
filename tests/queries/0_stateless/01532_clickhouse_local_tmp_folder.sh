#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# in case when clickhouse-local can't use temp folder it will try to create
# temporary subfolder in the current dir
TMP=/non-existent-folder-12123 ${CLICKHOUSE_LOCAL} -q 'SELECT 1'
