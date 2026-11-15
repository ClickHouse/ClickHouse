#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "SELECT * FROM format(Regexp, 's String', 'Hello')" 2>&1 | grep -o -F 'regular expression is not set'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM format(Regexp, 's String', 'Hello') SETTINGS format_regexp = 'Upyachka'" 2>&1 | grep -o -F '`Upyachka`'
