#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS database_03400;
CREATE DATABASE database_03400;
CREATE TABLE database_03400.allowed (a Int64, b Int64) Engine=MergeTree ORDER BY a;
CREATE TABLE database_03400.partial_allowed (a Int64, b Int64) Engine=MergeTree ORDER BY a;
CREATE TABLE database_03400.not_allowed (a Int64, b Int64) Engine=MergeTree ORDER BY a;
CREATE TABLE database_03400.no_show_allowed (a Int64, b Int64, c Int64) Engine=MergeTree ORDER BY a;

INSERT INTO database_03400.allowed SELECT number, number FROM numbers(10);
INSERT INTO database_03400.partial_allowed SELECT number + 10, number FROM numbers(10);
INSERT INTO database_03400.not_allowed SELECT number + 20, number FROM numbers(10);
INSERT INTO database_03400.no_show_allowed SELECT number + 30, number, number FROM numbers(10);

CREATE TABLE database_03400.merge Engine=Merge(database_03400, '.*allowed');
SELECT _table, * FROM database_03400.merge ORDER BY a SETTINGS enable_analyzer = 1;

DROP USER IF EXISTS user_test_03400;
CREATE USER user_test_03400 IDENTIFIED WITH plaintext_password BY 'user_test_03400';

GRANT TABLE ENGINE ON Merge TO 'user_test_03400';
GRANT CREATE TABLE ON database_03400.* TO 'user_test_03400';
GRANT SHOW ON database_03400.* TO 'user_test_03400';
REVOKE ALL ON database_03400.no_show_allowed FROM 'user_test_03400';
GRANT SELECT ON database_03400.allowed TO 'user_test_03400';
GRANT SELECT(a) ON database_03400.partial_allowed TO 'user_test_03400';
GRANT SELECT ON database_03400.merge* TO 'user_test_03400';
"""

echo "----Table engine"
# access from the Merge table
$CLICKHOUSE_CLIENT --multiline --user user_test_03400 --password user_test_03400 -q """
SELECT * FROM database_03400.merge; -- { serverError ACCESS_DENIED }
SELECT a FROM database_03400.merge; -- { serverError ACCESS_DENIED }
SELECT '----select allowed columns and databases';
SELECT _table, a FROM database_03400.merge WHERE _database='database_03400' AND _table IN ('allowed', 'partial_allowed')  ORDER BY a;
SELECT '----';
SELECT _table, a, b FROM database_03400.merge WHERE _database='database_03400' AND _table = 'allowed'  ORDER BY a;
SELECT '----';
SELECT _table, a FROM database_03400.merge WHERE _database='database_03400' AND _table IN ('allowed', 'no_show_allowed')  ORDER BY a;
SELECT '----create without show all';
CREATE TABLE database_03400.merge_user Engine=Merge(database_03400, '.*allowed');
SELECT name FROM system.columns WHERE database='database_03400' AND table='merge_user' ORDER BY name;
SELECT '----select user created table';
SELECT _table, * FROM database_03400.merge_user WHERE _table = 'allowed' ORDER BY a;
CREATE TABLE database_03400.merge_user_fail Engine=Merge(database_03400, 'no_show_allowed'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
"""

echo "----Table function"
# access from the Merge table function
$CLICKHOUSE_CLIENT --multiline --user user_test_03400 --password user_test_03400 -q """
SELECT * FROM merge(database_03400, '.*allowed'); -- { serverError ACCESS_DENIED }
SELECT a FROM merge(database_03400, '.*allowed'); -- { serverError ACCESS_DENIED }
SELECT '----select allowed columns and databases';
SELECT _table, a FROM merge(database_03400, '.*allowed') WHERE _table IN ('allowed', 'partial_allowed')  ORDER BY a;
SELECT '----';
SELECT _table, a, b FROM merge(database_03400, '.*allowed') WHERE _database='database_03400' AND _table = 'allowed'  ORDER BY a;
SELECT '----';
SELECT _table, a FROM merge(database_03400, '.*allowed') WHERE _table IN ('allowed', 'no_show_allowed')  ORDER BY a;
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS database_03400;
DROP USER IF EXISTS user_test_03400;
"""
