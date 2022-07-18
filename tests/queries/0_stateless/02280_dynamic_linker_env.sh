#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

static=$($CLICKHOUSE_LOCAL -q "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'STATIC'")

clickhouse-local -q 'select 1'

if [ "$static" -eq 1 ]; then
    # "grep -c" will also gives "1"
    LD_LIBRARY_PATH=/ clickhouse-local -q 'select 1' |& grep -x -F -c 'Environment variable LD_LIBRARY_PATH is set to /. It can compromise security.'
else
    # works because it does not uses main.cpp entrypoint
    # (due to shared build is always splitted, and non-splitted will have lots of ODR issues)
    LD_LIBRARY_PATH=/ clickhouse-local -q 'select 1'
fi
