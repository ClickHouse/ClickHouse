#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql client

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${MYSQL_CLIENT} --execute "SHOW WARNINGS;"
${MYSQL_CLIENT} --execute "show warnings;"
${MYSQL_CLIENT} --execute "SHOW WARNINGS LIMIT 100;"
${MYSQL_CLIENT} --execute "show warnings limit 100;"
${MYSQL_CLIENT} --execute "SHOW WARNINGS LIMIT 100 OFFSET 100;"
${MYSQL_CLIENT} --execute "show warnings limit 100 offset 100;"
${MYSQL_CLIENT} --execute "SHOW COUNT(*) WARNINGS;"
${MYSQL_CLIENT} --execute "show count(*) warnings;"
${MYSQL_CLIENT} --execute "SELECT @@session.warning_count;"
