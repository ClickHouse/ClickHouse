#!/usr/bin/env bash
# Tags: no-parallel

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP USER IF EXISTS user_test_02337;
CREATE USER user_test_02337 IDENTIFIED WITH plaintext_password BY 'user_test_02337';
REVOKE ALL ON *.* FROM user_test_02337;
"""
${CLICKHOUSE_CLIENT} --multiline --multiquery --user user_test_02337 --password user_test_02337 -q """
SYSTEM DROP FILESYSTEM CACHE; -- { serverError 497 }
"""
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
GRANT SYSTEM DROP FILESYSTEM CACHE ON *.* TO user_test_02337 WITH GRANT OPTION;
"""
${CLICKHOUSE_CLIENT} --multiline --multiquery --user user_test_02337 --password user_test_02337 -q """
SYSTEM DROP FILESYSTEM CACHE;
"""
