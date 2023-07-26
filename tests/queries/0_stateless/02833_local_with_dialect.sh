#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


echo "exit" | ${CLICKHOUSE_LOCAL} --query "from s\"SELECT * FROM numbers(1)\"" --dialect prql --interactive
