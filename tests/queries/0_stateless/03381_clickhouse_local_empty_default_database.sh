#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --database '' |& grep -o "default_database cannot be empty. (BAD_ARGUMENTS)"
$CLICKHOUSE_LOCAL -- --default_database='' |& grep -o "default_database cannot be empty. (BAD_ARGUMENTS)"
