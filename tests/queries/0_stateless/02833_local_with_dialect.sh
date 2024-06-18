#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# Remove last line since the good bye message changes depending on the date
echo "exit" | ${CLICKHOUSE_LOCAL} --query "from s\"SELECT * FROM numbers(1)\"" --dialect prql --interactive | head -n -1
