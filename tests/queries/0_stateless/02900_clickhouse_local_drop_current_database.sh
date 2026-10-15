#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --echo "
    CREATE DATABASE foo;
    USE foo;
    SELECT 1;
    DROP DATABASE foo;
    SELECT 2;
    USE default;
    SELECT 3;
"
