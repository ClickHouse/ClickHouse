#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql client

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${MYSQL_CLIENT} --verbose --execute "COMMIT;" | grep -c "COMMIT"
${MYSQL_CLIENT} --verbose --execute "ROLLBACK;" | grep -c "ROLLBACK"
