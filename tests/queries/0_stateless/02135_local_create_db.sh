#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for Engine in Atomic Ordinary; do
    $CLICKHOUSE_LOCAL --allow_deprecated_database_ordinary=1 --query """
    CREATE DATABASE foo_$Engine Engine=$Engine;
    DROP DATABASE foo_$Engine;
    """
done
