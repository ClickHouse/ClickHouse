#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "drop table if exists test_table"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_table
(
    a      UInt16 DEFAULT 0,
    c      LowCardinality(String) DEFAULT '',
    t_date LowCardinality(String) DEFAULT '',
    ex     LowCardinality(String) DEFAULT '',
    team   LowCardinality(String) DEFAULT '',
    g      LowCardinality(String) DEFAULT '',
    mt     FixedString(1) DEFAULT ' ',
    rw_ts  Int64 DEFAULT 0,
    exr_t  Int64 DEFAULT 0,
    en     UInt16 DEFAULT 0,
    f_t    Int64 DEFAULT 0,
    j      UInt64 DEFAULT 0,
    oj     UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY (c, t_date)
ORDER BY (ex, team, g, mt, rw_ts, exr_t, en, f_t, j, oj)
SETTINGS index_granularity = 8192"

$CLICKHOUSE_CLIENT --query "
INSERT INTO test_table(t_date, c,team, a) SELECT
arrayJoin([toDate('2021-07-15'),toDate('2021-07-16')]) as t_date,
arrayJoin(['aur','rua']) as c,
arrayJoin(['AWD','ZZZ']) as team,
arrayJoin([3183,3106,0,3130,3108,3126,3109,3107,3182,3180,3129,3128,3125,3266]) as a
FROM numbers(600);"

$CLICKHOUSE_CLIENT --query "DROP ROLE IF exists AWD;"
$CLICKHOUSE_CLIENT --query "create role AWD;"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM AWD;"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS AWD_user;"
$CLICKHOUSE_CLIENT --query "CREATE USER AWD_user IDENTIFIED WITH plaintext_password BY 'AWD_pwd' DEFAULT ROLE AWD;"

$CLICKHOUSE_CLIENT --query "GRANT SELECT ON test_table TO AWD;"

$CLICKHOUSE_CLIENT --query "DROP ROW POLICY IF EXISTS ttt_bu_test_table_AWD ON test_table;"
$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY ttt_bu_test_table_AWD ON test_table FOR SELECT USING team = 'AWD' TO AWD;"

$CLICKHOUSE_CLIENT  --user=AWD_user --password=AWD_pwd  --query "
SELECT count() AS count
 FROM test_table
WHERE
 t_date = '2021-07-15' AND c = 'aur' AND a=3130;
"

$CLICKHOUSE_CLIENT  --user=AWD_user --password=AWD_pwd  --query "
SELECT
    team,
    a,
    t_date,
    count() AS count
FROM test_table
WHERE (t_date = '2021-07-15') AND (c = 'aur') AND (a = 3130)
GROUP BY
    team,
    a,
    t_date;
"

$CLICKHOUSE_CLIENT  --user=AWD_user --password=AWD_pwd  --query "
SELECT count() AS count
FROM test_table
WHERE (t_date = '2021-07-15') AND (c = 'aur') AND (a = 313)
"
