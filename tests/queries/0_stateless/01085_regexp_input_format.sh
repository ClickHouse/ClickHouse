#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS regexp";
$CLICKHOUSE_CLIENT --query="CREATE TABLE regexp (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory";

echo 'id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03' | $CLICKHOUSE_CLIENT --query="INSERT INTO regexp SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped' FORMAT Regexp ";

echo 'id: 4 array: "[1,2,3]" string: "str4" date: "2020-01-04"
id: 5 array: "[1,2,3]" string: "str5" date: "2020-01-05"
id: 6 array: "[1,2,3]" string: "str6" date: "2020-01-06"' | $CLICKHOUSE_CLIENT --query="INSERT INTO regexp SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='CSV' FORMAT Regexp";

echo "id: 7 array: [1,2,3] string: 'str7' date: '2020-01-07'
id: 8 array: [1,2,3] string: 'str8' date: '2020-01-08'
id: 9 array: [1,2,3] string: 'str9' date: '2020-01-09'" | $CLICKHOUSE_CLIENT --query="INSERT INTO regexp SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Quoted' FORMAT Regexp";

echo 'id: 10 array: [1,2,3] string: "str10" date: "2020-01-10"
id: 11 array: [1,2,3] string: "str11" date: "2020-01-11"
id: 12 array: [1,2,3] string: "str12" date: "2020-01-12"' | $CLICKHOUSE_CLIENT --query="INSERT INTO regexp SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='JSON' FORMAT Regexp";

$CLICKHOUSE_CLIENT --query="SELECT * FROM regexp ORDER BY id";
$CLICKHOUSE_CLIENT --query="DROP TABLE regexp";

