#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS database_02416;
CREATE DATABASE database_02416;

DROP USER IF EXISTS user_test_02416;
CREATE USER user_test_02416 IDENTIFIED WITH plaintext_password BY 'user_test_02416';

GRANT CREATE DATABASE ON *.* TO 'user_test_02416' WITH GRANT OPTION;
GRANT DROP DATABASE ON *.* TO 'user_test_02416' WITH GRANT OPTION;
REVOKE DROP DATABASE ON database_02416.* FROM 'user_test_02416';
GRANT CREATE TABLE ON *.* TO 'user_test_02416' WITH GRANT OPTION;
GRANT DROP TABLE ON *.* TO 'user_test_02416' WITH GRANT OPTION;
"""
${CLICKHOUSE_CLIENT} --multiline --user user_test_02416 --password user_test_02416 -q """
RENAME DATABASE user_test_02416 to aaaaaaaaa; -- { serverError 497 }
"""
