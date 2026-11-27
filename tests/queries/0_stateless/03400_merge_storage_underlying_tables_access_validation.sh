#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_extra;
CREATE DATABASE ${CLICKHOUSE_DATABASE}_extra;
CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.allowed (a Int64, b Int64) Engine=MergeTree ORDER BY a;
CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.partial_allowed (a Int64, b Int64) Engine=MergeTree ORDER BY a;
CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.not_allowed (a Int64, b Int64) Engine=MergeTree ORDER BY a;
CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.no_show_allowed (a Int64, b Int64, c Int64) Engine=MergeTree ORDER BY a;

INSERT INTO ${CLICKHOUSE_DATABASE}_extra.allowed SELECT number, number FROM numbers(10);
INSERT INTO ${CLICKHOUSE_DATABASE}_extra.partial_allowed SELECT number + 10, number FROM numbers(10);
INSERT INTO ${CLICKHOUSE_DATABASE}_extra.not_allowed SELECT number + 20, number FROM numbers(10);
INSERT INTO ${CLICKHOUSE_DATABASE}_extra.no_show_allowed SELECT number + 30, number, number FROM numbers(10);

CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.merge Engine=Merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed');
SELECT _table, * FROM ${CLICKHOUSE_DATABASE}_extra.merge ORDER BY a SETTINGS enable_analyzer = 1;

DROP USER IF EXISTS user_${CLICKHOUSE_DATABASE};
CREATE USER user_${CLICKHOUSE_DATABASE} IDENTIFIED WITH plaintext_password BY 'user_${CLICKHOUSE_DATABASE}';

GRANT TABLE ENGINE ON Merge TO 'user_${CLICKHOUSE_DATABASE}';
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}_extra.* TO 'user_${CLICKHOUSE_DATABASE}';
GRANT SHOW ON ${CLICKHOUSE_DATABASE}_extra.* TO 'user_${CLICKHOUSE_DATABASE}';
REVOKE ALL ON ${CLICKHOUSE_DATABASE}_extra.no_show_allowed FROM 'user_${CLICKHOUSE_DATABASE}';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}_extra.allowed TO 'user_${CLICKHOUSE_DATABASE}';
GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}_extra.partial_allowed TO 'user_${CLICKHOUSE_DATABASE}';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}_extra.merge* TO 'user_${CLICKHOUSE_DATABASE}';
"""

echo "----Table engine"
# access from the Merge table
$CLICKHOUSE_CLIENT --multiline --user user_${CLICKHOUSE_DATABASE} --password user_${CLICKHOUSE_DATABASE} -q """
SELECT * FROM ${CLICKHOUSE_DATABASE}_extra.merge; -- { serverError ACCESS_DENIED }
SELECT a FROM ${CLICKHOUSE_DATABASE}_extra.merge; -- { serverError ACCESS_DENIED }
SELECT '----select allowed columns and databases';
SELECT _table, a FROM ${CLICKHOUSE_DATABASE}_extra.merge WHERE _database='${CLICKHOUSE_DATABASE}_extra' AND _table IN ('allowed', 'partial_allowed')  ORDER BY a;
SELECT '----';
SELECT _table, a, b FROM ${CLICKHOUSE_DATABASE}_extra.merge WHERE _database='${CLICKHOUSE_DATABASE}_extra' AND _table = 'allowed'  ORDER BY a;
SELECT '----';
SELECT _table, a FROM ${CLICKHOUSE_DATABASE}_extra.merge WHERE _database='${CLICKHOUSE_DATABASE}_extra' AND _table IN ('allowed', 'no_show_allowed')  ORDER BY a;
SELECT '----create without show all';
CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.merge_user Engine=Merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed');
SELECT name FROM system.columns WHERE database='${CLICKHOUSE_DATABASE}_extra' AND table='merge_user' ORDER BY name;
SELECT '----select user created table';
SELECT _table, * FROM ${CLICKHOUSE_DATABASE}_extra.merge_user WHERE _table = 'allowed' ORDER BY a;
CREATE TABLE ${CLICKHOUSE_DATABASE}_extra.merge_user_fail Engine=Merge(${CLICKHOUSE_DATABASE}_extra, 'no_show_allowed'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
"""

echo "----Table function"
# access from the Merge table function
$CLICKHOUSE_CLIENT --multiline --user user_${CLICKHOUSE_DATABASE} --password user_${CLICKHOUSE_DATABASE} -q """
SELECT * FROM merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed'); -- { serverError ACCESS_DENIED }
SELECT a FROM merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed'); -- { serverError ACCESS_DENIED }
SELECT '----select allowed columns and databases';
SELECT _table, a FROM merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed') WHERE _table IN ('allowed', 'partial_allowed')  ORDER BY a;
SELECT '----';
SELECT _table, a, b FROM merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed') WHERE _database='${CLICKHOUSE_DATABASE}_extra' AND _table = 'allowed'  ORDER BY a;
SELECT '----';
SELECT _table, a FROM merge(${CLICKHOUSE_DATABASE}_extra, '.*allowed') WHERE _table IN ('allowed', 'no_show_allowed')  ORDER BY a;
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_extra;
DROP USER IF EXISTS user_${CLICKHOUSE_DATABASE};
"""
