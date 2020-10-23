#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

echo "INSERT INTO test FORMAT CSV" | ${CLICKHOUSE_CLIENT} -n 2>/dev/null
echo $?
